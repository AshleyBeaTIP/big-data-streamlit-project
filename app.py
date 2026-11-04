import json
import os
import altair as alt
from collections import deque
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

load_dotenv()

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('MONGO_DB', 'bigdata')
REFRESH_MS = 15000
TZ = 'Asia/Manila'  # display timezone, change if needed
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
RANGES = {'Last 1 hour': 1, 'Last 6 hours': 6, 'Last 24 hours': 24, 'All time': None}

st.set_page_config(page_title='Crypto Stream Dashboard', page_icon='📈', layout='wide')


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------- Connections ----------
@st.cache_resource
def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[DB_NAME]


@st.cache_resource
def get_live_source():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode('utf-8')),
    )
    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    end = consumer.end_offsets([tp])[tp]
    consumer.seek(tp, max(0, end - 100))  # start from the last ~20 polls
    return consumer, deque(maxlen=500)


# ---------- Data functions ----------
def consume_kafka_data():
    consumer, buffer = get_live_source()
    for msgs in consumer.poll(timeout_ms=1000, max_records=500).values():
        for m in msgs:
            buffer.append(m.value)
    df = pd.DataFrame(list(buffer))
    if not df.empty:
        df['time'] = (pd.to_datetime(df['timestamp'], utc=True)
                      .dt.tz_convert(TZ).dt.tz_localize(None))
    return df


def query_historical_data(symbols, hours):
    query = {}
    if symbols:
        query['sensor_id'] = {'$in': symbols}
    if hours:
        query['ts'] = {'$gte': utc_now() - timedelta(hours=hours)}
    docs = list(get_db()['ticks'].find(query, {'_id': 0}).sort('ts', -1))
    df = pd.DataFrame(docs)
    if not df.empty:
        df['time'] = (pd.to_datetime(df['ts']).dt.tz_localize('UTC')
                      .dt.tz_convert(TZ).dt.tz_localize(None))
    return df


def query_aggregation(minutes=60):
    since = utc_now() - timedelta(minutes=minutes)
    docs = list(get_db()['agg_1min'].find({'window_start': {'$gte': since}}, {'_id': 0}))
    df = pd.DataFrame(docs)
    if df.empty:
        return df
    df['weighted'] = df['avg_price'] * df['records']
    g = df.groupby('sensor_id').agg(weighted=('weighted', 'sum'),
                                    records=('records', 'sum'),
                                    min_price=('min_price', 'min'),
                                    max_price=('max_price', 'max')).reset_index()
    g['avg_price'] = g['weighted'] / g['records']
    return g[['sensor_id', 'avg_price', 'min_price', 'max_price', 'records']]


# ---------- Pages ----------
def page_live():
    st_autorefresh(interval=REFRESH_MS, key='live_refresh')
    st.title('📈 Live Crypto Dashboard')
    try:
        df = consume_kafka_data()
    except Exception as e:
        st.error('Kafka error: ' + str(e))
        return
    if df.empty:
        st.info('Waiting for data... make sure producer.py is running.')
        return

    latest = df.sort_values('time').groupby('sensor_id').tail(1)
    st.caption('Last updated: ' + latest['time'].max().strftime('%Y-%m-%d %H:%M:%S')
               + '  (auto-refresh every 15 s)')

    cols = st.columns(len(latest))
    for col, (_, r) in zip(cols, latest.iterrows()):
        price = r['value']
        label = '${:,.2f}'.format(price) if price >= 10 else '${:,.4f}'.format(price)
        col.metric(r['sensor_id'].replace('USDT', ''), label,
                   '{:+.2f}%'.format(r['change_percent']))

    symbol = st.selectbox('Chart symbol', sorted(df['sensor_id'].unique()))
    st.subheader('Price over time')
    chart_df = df[df['sensor_id'] == symbol][['time', 'value']]
    chart = alt.Chart(chart_df).mark_line(point=True).encode(
        x=alt.X('time:T', title='Time'),
        y=alt.Y('value:Q', scale=alt.Scale(zero=False), title='Price (USDT)'),
        tooltip=[
            alt.Tooltip('time:T', title='Time', format='%Y-%m-%d %H:%M:%S'),
            alt.Tooltip('value:Q', title='Price (USDT)', format=',.4f'),
        ],
    )
    st.altair_chart(chart)

    st.subheader('Aggregation (last 60 minutes, computed by Spark)')
    try:
        agg = query_aggregation(60)
    except Exception as e:
        st.error('MongoDB error: ' + str(e))
        return
    if agg.empty:
        st.info('No aggregates yet. Wait about 1 to 2 minutes after starting Spark.')
    else:
        agg.columns = ['Symbol', 'Avg Price', 'Min', 'Max', 'Records']
        st.dataframe(agg, hide_index=True)


def page_history():
    st.title('📊 Data History')
    c1, c2 = st.columns(2)
    symbols = c1.multiselect('Symbol', SYMBOLS, default=SYMBOLS)
    label = c2.selectbox('Time range', list(RANGES))
    try:
        df = query_historical_data(symbols, RANGES[label])
    except Exception as e:
        st.error('MongoDB error: ' + str(e))
        return
    if df.empty:
        st.info('No records found for this filter yet.')
        return

    show = df[['time', 'sensor_id', 'value', 'change_percent', 'high', 'low', 'volume']]
    show = show.rename(columns={'time': 'Time', 'sensor_id': 'Symbol', 'value': 'Price',
                                'change_percent': 'Change %', 'high': '24h High',
                                'low': '24h Low', 'volume': 'Volume'})
    st.write('{:,} records'.format(len(show)))

    d1, d2, _ = st.columns([1, 1, 4])
    d1.download_button('Download CSV', show.to_csv(index=False).encode('utf-8'),
                       'crypto_history.csv', 'text/csv')
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        show.to_excel(writer, index=False, sheet_name='History')
    d2.download_button('Download Excel', buf.getvalue(), 'crypto_history.xlsx',
                       'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    st.dataframe(show, hide_index=True)


# ---------- Navigation ----------
page = st.sidebar.radio('Navigation', ['📈 Live', '📊 History'])
st.sidebar.caption('Source: Binance public market data (data source only)')
if page.endswith('Live'):
    page_live()
else:
    page_history()