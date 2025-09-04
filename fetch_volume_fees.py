# fetch_volume_fees.py
"""
Завдання:
 - тягнути markets endpoint Extended API
 - підставляти поля (fetched_at, market, daily_volume, dailyVolumeBase, openInterest, fundingRate, lastPrice, bidPrice, askPrice, markPrice, indexPrice)
 - додає нові рядки в data/volume_fees.csv (append)
 - уникає дублів (де дубль = same fetched_at + market) — залишає найсвіжчий рядок
"""

import requests
import pandas as pd
import os
from datetime import datetime

API_URL = "https://api.starknet.extended.exchange/api/v1/info/markets"  # або інший робочий endpoint; підставляй при потребі
OUT_DIR = "data"
OUT_FILE = os.path.join(OUT_DIR, "volume_fees.csv")
TIMEOUT = 20

def fetch_markets():
    try:
        r = requests.get(API_URL, timeout=TIMEOUT)
        r.raise_for_status()
        payload = r.json()
        markets = payload.get("data") if isinstance(payload, dict) else payload
        return markets or []
    except Exception as e:
        print("❌ API Error:", e)
        return []

def normalize_markets(markets):
    rows = []
    fetched_at = datetime.utcnow().isoformat() + "Z"
    for m in markets:
        stats = m.get("marketStats") if isinstance(m, dict) else {}
        rows.append({
            "fetched_at": fetched_at,
            "market": m.get("name") if isinstance(m, dict) else str(m),
            "dailyVolume": stats.get("dailyVolume") or None,
            "dailyVolumeBase": stats.get("dailyVolumeBase") or None,
            "openInterest": stats.get("openInterest") or stats.get("openInterestBase") or None,
            "fundingRate": stats.get("fundingRate") or None,
            "lastPrice": stats.get("lastPrice") or None,
            "bidPrice": stats.get("bidPrice") or None,
            "askPrice": stats.get("askPrice") or None,
            "markPrice": stats.get("markPrice") or None,
            "indexPrice": stats.get("indexPrice") or None
        })
    return rows

def ensure_csv_and_append(new_df):
    os.makedirs(OUT_DIR, exist_ok=True)
    if not os.path.exists(OUT_FILE):
        new_df.to_csv(OUT_FILE, index=False)
        print("Created new CSV with", len(new_df), "rows.")
        return

    # якщо файл існує — зчитай, append, dedupe
    existing = pd.read_csv(OUT_FILE, dtype=str)
    combined = pd.concat([existing, new_df], ignore_index=True)

    # Унікальність: fetched_at + market (залишаємо останній рядок для тієї пари)
    # Але fetched_at має ISO timestamp — якщо ти запускаєш кожен раз унікальний fetched_at → дублів за часом не буде.
    # Далі використовуємо sort щоб останні записи були зверху
    combined['fetched_at_sort'] = pd.to_datetime(combined['fetched_at'], errors='coerce')
    combined.sort_values(['market','fetched_at_sort'], ascending=[True, True], inplace=True)
    deduped = combined.drop_duplicates(subset=['market','fetched_at'], keep='last')  # keep last (most recent)
    deduped = deduped.drop(columns=['fetched_at_sort'])
    deduped.to_csv(OUT_FILE, index=False)
    print("Appended and deduped. Total rows now:", len(deduped))

def main():
    markets = fetch_markets()
    rows = normalize_markets(markets)
    if not rows:
        # створюємо порожній рядок якщо зовсім немає даних (щоб Dune не падало)
        print("No markets returned; writing an empty CSV (if missing) or doing nothing.")
        # не перезаписуємо існуючий файл
        return
    df = pd.DataFrame(rows)
    ensure_csv_and_append(df)

if __name__ == "__main__":
    main()
