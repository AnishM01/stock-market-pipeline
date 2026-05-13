import io
import os
from datetime import datetime

import pandas as pd
import yfinance as yf
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "bronze")

TICKERS = [
    # FAANG + Big Tech
    "META", "AAPL", "AMZN", "NFLX", "GOOGL", "MSFT", "NVDA", "AMD", "INTC", "TSLA",
    # Finance
    "JPM", "GS", "BAC", "MS", "V", "MA", "AXP", "BLK",
    # Healthcare
    "JNJ", "PFE", "UNH", "ABBV", "MRK",
    # Retail
    "WMT", "TGT", "NKE", "MCD", "SBUX", "COST",
    # Energy
    "XOM", "CVX", "COP",
    # ETFs
    "SPY", "QQQ", "DIA", "IWM", "VTI",
]


def validate_env():
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING is not set in .env")
    if not AZURE_CONTAINER_NAME:
        raise EnvironmentError("AZURE_CONTAINER_NAME is not set in .env")


def fetch_stock_data(ticker: str) -> pd.DataFrame:
    stock = yf.Ticker(ticker)
    df = stock.history(period="1y", interval="1d", auto_adjust=True)
    if df.empty:
        raise ValueError(f"No data returned for {ticker}")
    df.reset_index(inplace=True)
    # Normalize timezone-aware datetime to UTC date string to avoid parquet schema issues
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
    df["Ticker"] = ticker
    # Drop yfinance metadata columns that aren't standard OHLCV
    df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]
    return df


def upload_to_adls(df: pd.DataFrame, ticker: str, service_client: DataLakeServiceClient):
    fs_client = service_client.get_file_system_client(AZURE_CONTAINER_NAME)
    today = datetime.today().strftime("%Y-%m-%d")
    dir_path = f"stocks/{ticker}/date={today}"
    file_name = f"{ticker}_{today}.parquet"

    dir_client = fs_client.get_directory_client(dir_path)
    dir_client.create_directory()

    file_client = dir_client.get_file_client(file_name)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    data = buffer.getvalue()

    file_client.upload_data(data, overwrite=True, length=len(data))
    print(f"  [OK] {ticker} -> {dir_path}/{file_name} ({len(df)} rows, {len(data):,} bytes)")


def main():
    validate_env()

    service_client = DataLakeServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

    print(f"Starting ingestion for {len(TICKERS)} tickers at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)

    succeeded, failed = [], []

    for ticker in TICKERS:
        try:
            df = fetch_stock_data(ticker)
            upload_to_adls(df, ticker, service_client)
            succeeded.append(ticker)
        except Exception as e:
            print(f"  [FAIL] {ticker}: {e}")
            failed.append((ticker, str(e)))

    print("-" * 60)
    print(f"Ingestion complete: {len(succeeded)}/{len(TICKERS)} succeeded")
    if failed:
        print("Failed tickers:")
        for ticker, err in failed:
            print(f"  {ticker}: {err}")


if __name__ == "__main__":
    main()
