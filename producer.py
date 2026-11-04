import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')
INTERVAL = int(os.getenv('POLL_SECONDS', '15'))
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
URL = 'https://data-api.binance.vision/api/v3/ticker/24hr'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def make_producer():
    # Keep trying until Kafka is reachable
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8'),
                retries=3,
            )
        except KafkaError as e:
            logging.error('Kafka not ready (%s). Retrying in 5s...', e)
            time.sleep(5)


def fetch_prices():
    params = {'symbols': json.dumps(SYMBOLS, separators=(',', ':'))}
    resp = requests.get(URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def to_record(t, now_iso):
    return {
        'timestamp': now_iso,
        'value': float(t['lastPrice']),
        'metric_type': 'price',
        'sensor_id': t['symbol'],
        'change_percent': float(t['priceChangePercent']),
        'high': float(t['highPrice']),
        'low': float(t['lowPrice']),
        'volume': float(t['volume']),
    }


def main():
    producer = make_producer()
    logging.info('Producer started. Topic=%s, every %ss', TOPIC, INTERVAL)
    while True:
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            for t in fetch_prices():
                rec = to_record(t, now_iso)
                producer.send(TOPIC, key=rec['sensor_id'], value=rec)
                logging.info('sent %s %.4f', rec['sensor_id'], rec['value'])
            producer.flush()
        except (requests.RequestException, KeyError, ValueError) as e:
            logging.warning('API or parse error: %s', e)
        except KafkaError as e:
            logging.error('Kafka error: %s', e)
        time.sleep(INTERVAL)


if __name__ == '__main__':
    main()