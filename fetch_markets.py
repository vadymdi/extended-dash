import requests
import pandas as pd
import os

API_URL = "https://api.extended.exchange/v1/markets"

def fetch_markets():
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Помилка API: {e}")
        data = []

    # Якщо даних немає, створюємо порожній DataFrame з колонками
    if not data:
        print("⚠️ Дані відсутні, створюємо порожній CSV")
        df = pd.DataFrame(columns=["id", "name", "symbol", "price", "volume", "fees"])
    else:
        df = pd.DataFrame(data)
    
    # Створюємо папку data, якщо її немає
    os.makedirs("data", exist_ok=True)
    output_file = "data/markets.csv"
    df.to_csv(output_file, index=False)

    print(f"✅ CSV збережено у {output_file}")
    print(df.head())

if __name__ == "__main__":
    fetch_markets()
