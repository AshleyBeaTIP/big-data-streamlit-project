import os

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, col, count, from_json, max as smax,
                                   min as smin, to_timestamp, window)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA = os.getenv('KAFKA_BOOTSTRAP', 'kafka:19092')
TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017')
DB_NAME = os.getenv('MONGO_DB', 'bigdata')

spark = (SparkSession.builder.appName('CryptoStream')
         .config('spark.sql.shuffle.partitions', '2')
         .config('spark.sql.session.timeZone', 'UTC')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

schema = StructType([
    StructField('timestamp', StringType()),
    StructField('value', DoubleType()),
    StructField('metric_type', StringType()),
    StructField('sensor_id', StringType()),
    StructField('change_percent', DoubleType()),
    StructField('high', DoubleType()),
    StructField('low', DoubleType()),
    StructField('volume', DoubleType()),
])

raw = (spark.readStream.format('kafka')
       .option('kafka.bootstrap.servers', KAFKA)
       .option('subscribe', TOPIC)
       .option('startingOffsets', 'latest')
       .option('failOnDataLoss', 'false')
       .load())

# Parse JSON, drop bad rows, add a real timestamp column
parsed = (raw.select(from_json(col('value').cast('string'), schema).alias('d'))
          .select('d.*')
          .dropna(subset=['timestamp', 'value', 'sensor_id'])
          .withColumn('ts', to_timestamp('timestamp')))


def save_raw(df, batch_id):
    rows = [r.asDict() for r in df.collect()]
    if rows:
        MongoClient(MONGO_URI)[DB_NAME]['ticks'].insert_many(rows)


agg = (parsed.withWatermark('ts', '2 minutes')
       .groupBy(window('ts', '1 minute'), col('sensor_id'))
       .agg(avg('value').alias('avg_price'),
            smin('value').alias('min_price'),
            smax('value').alias('max_price'),
            count('*').alias('records')))


def save_agg(df, batch_id):
    coll = MongoClient(MONGO_URI)[DB_NAME]['agg_1min']
    for r in df.collect():
        coll.update_one(
            {'sensor_id': r['sensor_id'], 'window_start': r['window']['start']},
            {'$set': {'window_end': r['window']['end'],
                      'avg_price': r['avg_price'],
                      'min_price': r['min_price'],
                      'max_price': r['max_price'],
                      'records': r['records']}},
            upsert=True)


raw_query = (parsed.writeStream.foreachBatch(save_raw)
             .option('checkpointLocation', '/tmp/chk_raw')
             .trigger(processingTime='10 seconds').start())

agg_query = (agg.writeStream.foreachBatch(save_agg)
             .outputMode('update')
             .option('checkpointLocation', '/tmp/chk_agg')
             .trigger(processingTime='10 seconds').start())

spark.streams.awaitAnyTermination()