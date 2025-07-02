from binance.client import Client
from ConfigBinance.Config import Config
import pandas as pd
from datetime import datetime
from tqdm import tqdm

interval_ms = {
        "1s": 1000,  # 1 sec
        "1m": 60000,  # 1 min
        "5m": 300000,  # 5 min
        "15m": 900000,  # 15 min
        "30m": 1800000,  # 30 min
        "1h": 3600000,  # 1 hour
        "4h": 14400000,  # 4 hours
        "1d": 86400000  # 1 day
    }

client = Client(Config.BINANCE_API_KEY, Config.BINANCE_API_SECRET, testnet=False)  # Create a client to connect to Binance API
def to_timestamp(date_string):
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    timestamp = datetime.timestamp(date_object)
    timestamp_ms = int(timestamp * 1000)
    return timestamp_ms

def generate_batches(start_date, end_date, interval, limit_per_request=1500):
    start_timestamp = to_timestamp(start_date)
    end_timestamp = to_timestamp(end_date)
    step = interval_ms[interval]  # Get the interval in milliseconds
    batches = []
    current_start = start_timestamp

    while current_start < end_timestamp:
        current_end = min(current_start + step * limit_per_request, end_timestamp)
        batches.append((current_start, current_end))
        current_start = current_end

    return batches

def get_batched_historical_klines(symbol, interval, start_date, end_date=None, limit_per_request=1500):
    if end_date is None:
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
    batches = generate_batches(start_date, end_date, interval, limit_per_request)
    all_klines = []

    for start, end in tqdm(batches, desc="Fetching historical klines"):
        klines = client.get_historical_klines(symbol, interval, start_str=start, end_str=end)
        all_klines.extend(klines)

    return all_klines

#klines = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "2024-05-10")
klines = get_batched_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "2025-06-10", limit_per_request=3000)

df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                                'taker_buy_quote_asset_volume', 'ignore'])

df['datetime'] = df['datetime'].values.astype(dtype='datetime64[ms]')
df['open'] = df['open'].values.astype(float)
df['high'] = df['high'].values.astype(float)
df['low'] = df['low'].values.astype(float)
df['close'] = df['close'].values.astype(float)
df['volume'] = df['volume'].values.astype(float)

# df = df[:-1]  # to skip last candle
df.to_csv('historical_data.csv')
print(df)
