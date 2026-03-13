"""
RedAlert -> Power BI Data Connector  (v3 - fixed translations + clean types)
Run this first, then run redalert_enrich.py
"""

import requests
import pandas as pd
from datetime import datetime, timezone
import time
import os

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
API_KEY     = "pr_YuWyayWasifRoKJacjkfCBRHEyifrRyGSZjgbdJPJKjHIFXLrNpLuyathksypUKD"
BASE_URL    = "https://redalert.orielhaim.com"
START_DATE  = "2026-02-28T00:00:00Z"
END_DATE    = None
OUTPUT_FILE = "redalert_data.csv"
BATCH_SIZE  = 100

ALERT_TYPE_MAP = {
    "missiles":                      {"label": "Missiles / Rockets",       "category": "RA"},
    "hostileAircraftIntrusion":      {"label": "Hostile Aircraft",          "category": "RA"},
    "terroristInfiltration":         {"label": "Terrorist Infiltration",    "category": "RA"},
    "radiologicalEvent":             {"label": "Radiological Event",        "category": "RA"},
    "hazardousMaterials":            {"label": "Hazardous Materials",       "category": "RA"},
    "earthQuake":                    {"label": "Earthquake",                "category": "RA"},
    "tsunami":                       {"label": "Tsunami",                   "category": "RA"},
    "newsFlash":                     {"label": "Pre-Alert (News Flash)",    "category": "PA"},
    "endAlert":                      {"label": "All Clear",                 "category": "AC"},
    "missilesDrill":                 {"label": "Drill: Missiles",           "category": "Drill"},
    "radiologicalEventDrill":        {"label": "Drill: Radiological",       "category": "Drill"},
    "earthQuakeDrill":               {"label": "Drill: Earthquake",         "category": "Drill"},
    "tsunamiDrill":                  {"label": "Drill: Tsunami",            "category": "Drill"},
    "hostileAircraftIntrusionDrill": {"label": "Drill: Hostile Aircraft",   "category": "Drill"},
    "hazardousMaterialsDrill":       {"label": "Drill: Hazmat",             "category": "Drill"},
    "terroristInfiltrationDrill":    {"label": "Drill: Terrorist",          "category": "Drill"},
}

def get_headers():
    return {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}

def fetch_history_page(offset, start_date, end_date):
    query_parts = [
        f"limit={BATCH_SIZE}",
        f"offset={offset}",
        "include=translations,coords",
        "order=asc",
        "sort=timestamp",
    ]
    if start_date:
        query_parts.append(f"startDate={start_date}")
    if end_date:
        query_parts.append(f"endDate={end_date}")

    url = f"{BASE_URL}/api/stats/history?" + "&".join(query_parts)
    response = requests.get(url, headers=get_headers(), timeout=30)

    if response.status_code in (400, 401, 403):
        print(f"\n❌ {response.status_code} Error. Body: {response.text[:300]}")
        response.raise_for_status()

    response.raise_for_status()
    return response.json()

def extract_english(translations, hebrew_name):
    """
    API returns translations as:
      translations.name = {"en": "...", "ru": "...", "ar": "..."}
      translations.zone = {"en": "...", ...}
    Fall back to Hebrew if English not available.
    """
    if not translations:
        return hebrew_name, None

    # translations.name can be a dict or a string
    name_obj = translations.get("name")
    if isinstance(name_obj, dict):
        city_en = name_obj.get("en") or hebrew_name
    elif isinstance(name_obj, str):
        city_en = name_obj or hebrew_name
    else:
        city_en = hebrew_name

    zone_obj = translations.get("zone")
    if isinstance(zone_obj, dict):
        zone_en = zone_obj.get("en")
    elif isinstance(zone_obj, str):
        zone_en = zone_obj
    else:
        zone_en = None

    return city_en, zone_en

def fetch_all_history(start_date=START_DATE, end_date=END_DATE):
    rows = []
    offset = 0
    total = None

    print(f"Fetching alert history from {start_date or 'all time'} to {end_date or 'now'}...")

    while True:
        data = fetch_history_page(offset, start_date, end_date)
        pagination = data.get("pagination", {})

        if total is None:
            total = pagination.get("total", 0)
            print(f"Total alerts to fetch: {total:,}")

        alerts = data.get("data", [])
        if not alerts:
            break

        for alert in alerts:
            alert_id       = alert.get("id")
            timestamp_raw  = alert.get("timestamp")
            alert_type_raw = alert.get("type", "")
            origin         = alert.get("origin") or "Unknown"

            type_info      = ALERT_TYPE_MAP.get(alert_type_raw, {})
            alert_label    = type_info.get("label", alert_type_raw)
            alert_category = type_info.get("category", "Other")

            try:
                ts       = datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
                date_str = ts.strftime("%Y-%m-%d")
                time_str = ts.strftime("%H:%M:%S")
            except Exception:
                date_str = None
                time_str = None

            cities = alert.get("cities", [])
            if not cities:
                rows.append({
                    "alert_id":       alert_id,
                    "timestamp":      timestamp_raw,
                    "date":           date_str,
                    "time":           time_str,
                    "type_raw":       alert_type_raw,
                    "type_label":     alert_label,
                    "alert_category": alert_category,
                    "origin":         origin,
                    "city_id":        None,
                    "city_hebrew":    None,
                    "city_english":   None,
                    "city_zone":      None,
                    "lat":            None,
                    "lng":            None,
                })
            else:
                for city in cities:
                    hebrew_name  = city.get("name")
                    translations = city.get("translations") or {}
                    city_en, zone_en = extract_english(translations, hebrew_name)

                    rows.append({
                        "alert_id":       alert_id,
                        "timestamp":      timestamp_raw,
                        "date":           date_str,
                        "time":           time_str,
                        "type_raw":       alert_type_raw,
                        "type_label":     alert_label,
                        "alert_category": alert_category,
                        "origin":         origin,
                        "city_id":        city.get("id"),
                        "city_hebrew":    hebrew_name,
                        "city_english":   city_en,
                        "city_zone":      zone_en,
                        "lat":            city.get("lat"),
                        "lng":            city.get("lng"),
                    })

        fetched_so_far = offset + len(alerts)
        print(f"  Fetched {fetched_so_far:,} / {total:,} alerts...", end="\r")

        if not pagination.get("hasMore", False):
            break

        offset += BATCH_SIZE
        time.sleep(0.2)

    print(f"\nDone. {len(rows):,} rows collected.")
    return rows

def main():
    if API_KEY == "your-api-key-here":
        print("ERROR: Set your API key in the API_KEY variable.")
        return

    rows = fetch_all_history()
    if not rows:
        print("No data returned.")
        return

    df = pd.DataFrame(rows)

    # Clean types — proper nulls, not empty strings
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
    df["date"]      = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["city_id"]   = pd.to_numeric(df["city_id"],  errors="coerce").astype("Int64")
    df["lat"]       = pd.to_numeric(df["lat"],       errors="coerce")
    df["lng"]       = pd.to_numeric(df["lng"],       errors="coerce")

    df = df.sort_values("timestamp").reset_index(drop=True)

    # Spot-check translations
    sample = df[df["city_english"].notna() & (df["city_english"] != df["city_hebrew"])].head(3)
    print("\nTranslation spot-check (Hebrew → English):")
    if len(sample):
        for _, r in sample.iterrows():
            print(f"  {r['city_hebrew']} → {r['city_english']}")
    else:
        print("  ⚠️  No translated rows found — API may not be returning translations.")
        print("     city_english will fall back to Hebrew names.")

    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ Saved {output_path}  ({len(df):,} rows)")

if __name__ == "__main__":
    main()