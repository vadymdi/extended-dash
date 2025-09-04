import requests
import pandas as pd
import os

API_URL = "https://api.extended.exchange/api/v1/info/markets"

def fetch_volume_fees():
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        markets = data.get("data", [])
    except Exception as e:
        print(f"❌ Помилка API: {e}")
        markets = []

    rows = []
    for m in markets:
        rows.append({
            "market": m.get("name"),
            "dailyVolume": m.get("marketStats", {}).get("dailyVolume"),
            "dailyVolumeBase": m.get("marketStats", {}).get("dailyVolumeBase"),
            "fees": m.get("marketStats", {}).get("fees")  # може бути None
        })

    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    output_file = "data/volume_fees.csv"
    df.to_csv(output_file, index=False)
    print(f"✅ CSV збережено у {output_file}")
    print(df.head())

if __name__ == "__main__":
    fetch_volume_fees()
