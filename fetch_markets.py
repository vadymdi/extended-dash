import requests
import pandas as pd
import os

API_URL = "https://api.extended.exchange/v1/markets"

def fetch_markets():
    response = requests.get(API_URL)
    data = response.json()

    df = pd.DataFrame(data)

    os.makedirs("data", exist_ok=True)
    output_file = "data/markets.csv"
    df.to_csv(output_file, index=False)

    print(f"✅ Дані збережено у {output_file}")
    print(df.head())

if __name__ == "__main__":
    fetch_markets()
