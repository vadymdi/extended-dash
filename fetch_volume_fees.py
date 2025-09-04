# fetch_volume_fees.py
import requests
import pandas as pd
import os
from datetime import datetime

# candidate endpoints (fallbacks) — спробуємо по порядку
ENDPOINTS = [
    "https://api.extended.exchange/v1/info/markets",
    "https://api.extended.exchange/api/v1/info/markets",
    "https://api.starknet.extended.exchange/api/v1/info/markets",
    "https://api.docs.extended.exchange/api/v1/info/markets"
]

OUT_DIR = "data"
OUT_FILE = os.path.join(OUT_DIR, "volume_fees.csv")
TIMEOUT = 20

def fetch_from(url):
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"⚠️ Request to {url} failed: {e}")
        return None

def normalize(markets_raw):
    rows = []
    for m in markets_raw:
        # allow that m might be a string (market id) or dict
        if isinstance(m, str):
            rows.append({
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "market": m,
                "dailyVolume": None,
                "dailyVolumeBase": None,
                "openInterest": None,
                "fundingRate": None
            })
            continue

        # m is dict
        stats = {}
        # sometimes stats are directly under m["marketStats"] or m["stats"]
        if isinstance(m, dict):
            stats = m.get("marketStats") or m.get("stats") or {}
        rows.append({
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "market": m.get("name") or m.get("id") or m.get("market") or None,
            "dailyVolume": stats.get("dailyVolume") or stats.get("daily_volume") or None,
            "dailyVolumeBase": stats.get("dailyVolumeBase") or stats.get("daily_volume_base") or None,
            "openInterest": stats.get("openInterest") or stats.get("open_interest") or None,
            "fundingRate": stats.get("fundingRate") or stats.get("funding_rate") or None
        })
    return rows

def main():
    data = None
    used_endpoint = None
    for url in ENDPOINTS:
        print(f"Trying {url} ...")
        resp = fetch_from(url)
        if not resp:
            continue
        # if response is dict with 'data'
        if isinstance(resp, dict) and "data" in resp:
            markets = resp.get("data") or []
            used_endpoint = url
            data = markets
            break
        # if response is a list directly
        if isinstance(resp, list):
            used_endpoint = url
            data = resp
            break
        # else continue to next
    if data is None:
        print("❌ No usable response from endpoints. Creating empty CSV with headers.")
        data = []

    rows = normalize(data)
    df = pd.DataFrame(rows, columns=["fetched_at", "market", "dailyVolume", "dailyVolumeBase", "openInterest", "fundingRate"])

    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(OUT_FILE, index=False)
    print(f"✅ CSV saved: {OUT_FILE} (rows: {len(df)})")
    if used_endpoint:
        print(f"Used endpoint: {used_endpoint}")
    else:
        print("Used endpoint: none (empty CSV)")

if __name__ == "__main__":
    main()
