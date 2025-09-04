import requests
import pandas as pd
from datetime import datetime

def fetch_volume_fees():
    url = "https://api.extended.exchange/v1/info/markets"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("data", [])
    except Exception as e:
        print(f"❌ Помилка API: {e}")
        data = []

    records = []
    for m in data:
        stats = m.get("marketStats", {})
        records.append({
            "market": m.get("name"),
            "dailyVolume": stats.get("dailyVolume"),
            "dailyVolumeBase": stats.get("dailyVolumeBase"),
            "timestamp": datetime.utcnow()
        })

    df = pd.DataFrame(records)
    df.to_csv("data/volume_fees.csv", index=False)
    print("✅ CSV збережено у data/volume_fees.csv")
    print(df)

if __name__ == "__main__":
    fetch_volume_fees()
