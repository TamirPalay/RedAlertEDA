"""
RedAlert Data Enrichment Script (v4 - no nulls, all numeric)
Run AFTER redalert_to_powerbi.py

Key change: NaN/None replaced with -1 sentinel so Power BI
reads all columns as numbers with no type conversion errors.
In DAX, filter out -1 where needed.
"""

import pandas as pd
import numpy as np
from datetime import timedelta

INPUT_FILE       = "redalert_data.csv"
OUTPUT_MAIN      = "redalert_data_enriched.csv"
OUTPUT_SEQUENCES = "redalert_sequences.csv"
PA_TO_RA_WINDOW  = timedelta(minutes=15)

NULL_INT   = 0       # sentinel for missing integer/flag values
NULL_FLOAT = 0.0     # sentinel for missing float values

# ── LOAD ──────────────────────────────────────
print("Loading data...")
df = pd.read_csv(INPUT_FILE)
df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
df = df.sort_values("timestamp").reset_index(drop=True)
print(f"  Loaded {len(df):,} rows")
print(f"  Categories: {df['alert_category'].value_counts().to_dict()}")

# ── BUILD SEQUENCES ───────────────────────────
print("\nBuilding sequences per city...")

df_city = df[
    df["alert_category"].isin(["PA","RA","AC"]) & df["city_english"].notna()
].copy()

sequences     = []
alert_seq_map = {}
seq_counter   = [0]

def flush(s, city, seq_counter, sequences, alert_seq_map, ac_time=None, ac_idx=None):
    if s["pa_time"] is None and s["ra_time"] is None:
        s["state"] = "IDLE"; s["pa_time"] = None; s["pa_idx"] = None
        s["ra_time"] = None; s["ra_idxs"] = []
        return

    seq_counter[0] += 1
    sid    = seq_counter[0]
    has_pa = s["pa_time"] is not None
    has_ra = s["ra_time"] is not None
    has_ac = ac_time is not None

    pa_to_ra_sec = (s["ra_time"] - s["pa_time"]).total_seconds() if has_pa and has_ra else None
    ra_to_ac_sec = (ac_time - s["ra_time"]).total_seconds()      if has_ra and has_ac else None

    sequences.append({
        "sequence_id":   sid,
        "city":          city,
        "pa_time":       s["pa_time"].isoformat() if s["pa_time"] else "",
        "first_ra_time": s["ra_time"].isoformat() if s["ra_time"] else "",
        "ac_time":       ac_time.isoformat()       if ac_time     else "",
        # Flags — always 0 or 1, never null
        "has_pa":        1 if has_pa else 0,
        "has_ra":        1 if has_ra else 0,
        "has_ac":        1 if has_ac else 0,
        "ra_with_pa":    1 if (has_ra and has_pa)     else 0,
        "ra_without_pa": 1 if (has_ra and not has_pa) else 0,
        "pa_without_ra": 1 if (has_pa and not has_ra) else 0,
        # Timing — use -1 sentinel when not applicable
        "pa_to_ra_sec":  round(pa_to_ra_sec, 2)      if pa_to_ra_sec is not None else NULL_FLOAT,
        "ra_to_ac_sec":  round(ra_to_ac_sec, 2)      if ra_to_ac_sec is not None else NULL_FLOAT,
        "pa_to_ra_min":  round(pa_to_ra_sec / 60, 2) if pa_to_ra_sec is not None else NULL_FLOAT,
        "ra_to_ac_min":  round(ra_to_ac_sec / 60, 2) if ra_to_ac_sec is not None else NULL_FLOAT,
    })

    if s["pa_idx"] is not None:
        alert_seq_map[s["pa_idx"]] = sid
    for idx in s["ra_idxs"]:
        alert_seq_map[idx] = sid
    if ac_idx is not None:
        alert_seq_map[ac_idx] = sid

    s["state"] = "IDLE"; s["pa_time"] = None; s["pa_idx"] = None
    s["ra_time"] = None; s["ra_idxs"] = []

cities = df_city["city_english"].unique()
for i, city in enumerate(cities):
    if i % 200 == 0:
        print(f"  Processing city {i}/{len(cities)}...", end="\r")

    city_rows = df_city[df_city["city_english"] == city].sort_values("timestamp")
    s = {"state":"IDLE","pa_time":None,"pa_idx":None,"ra_time":None,"ra_idxs":[]}

    for row in city_rows.itertuples():
        cat = row.alert_category
        ts  = row.timestamp
        idx = row.Index

        if cat == "AC":
            flush(s, city, seq_counter, sequences, alert_seq_map, ac_time=ts, ac_idx=idx)
        elif cat == "PA":
            if s["state"] in ("IDLE","RA_ACTIVE"):
                flush(s, city, seq_counter, sequences, alert_seq_map)
            s["state"] = "PA_PENDING"; s["pa_time"] = ts; s["pa_idx"] = idx
        elif cat == "RA":
            if s["state"] == "IDLE":
                s["state"] = "RA_ACTIVE"; s["ra_time"] = ts; s["ra_idxs"] = [idx]
            elif s["state"] == "PA_PENDING":
                if (ts - s["pa_time"]) <= PA_TO_RA_WINDOW:
                    s["state"] = "RA_ACTIVE"; s["ra_time"] = ts; s["ra_idxs"] = [idx]
                else:
                    flush(s, city, seq_counter, sequences, alert_seq_map)
                    s["state"] = "RA_ACTIVE"; s["ra_time"] = ts; s["ra_idxs"] = [idx]
            elif s["state"] == "RA_ACTIVE":
                s["ra_idxs"].append(idx)

    flush(s, city, seq_counter, sequences, alert_seq_map)

print(f"\n  Built {len(sequences):,} sequences across {len(cities):,} cities")

# ── SEQUENCES DATAFRAME ───────────────────────
seq_df = pd.DataFrame(sequences)

# All numeric columns — force to float, no nulls
for col in ["pa_to_ra_sec","ra_to_ac_sec","pa_to_ra_min","ra_to_ac_min"]:
    seq_df[col] = pd.to_numeric(seq_df[col], errors="coerce").fillna(NULL_FLOAT)

for col in ["sequence_id","has_pa","has_ra","has_ac","ra_with_pa","ra_without_pa","pa_without_ra"]:
    seq_df[col] = pd.to_numeric(seq_df[col], errors="coerce").fillna(NULL_INT)

print("\nSequence summary:")
print(f"  RA with PA:    {(seq_df['ra_with_pa'] == 1).sum():,}")
print(f"  RA without PA: {(seq_df['ra_without_pa'] == 1).sum():,}")
print(f"  PA without RA: {(seq_df['pa_without_ra'] == 1).sum():,}")

pa_ra = seq_df[seq_df["pa_to_ra_min"] > 0]
ra_ac = seq_df[seq_df["ra_to_ac_min"] > 0]
print(f"  Avg PA->RA: {pa_ra['pa_to_ra_min'].mean():.1f} min  (n={len(pa_ra):,})")
print(f"  Avg RA->AC: {ra_ac['ra_to_ac_min'].mean():.1f} min  (n={len(ra_ac):,})")

# ── ENRICH MAIN TABLE ─────────────────────────
print("\nEnriching main table...")
df["sequence_id"] = df.index.map(alert_seq_map)

seq_lookup = seq_df.set_index("sequence_id")[
    ["ra_with_pa","ra_without_pa","pa_without_ra","pa_to_ra_min","ra_to_ac_min"]
].to_dict("index")

def get_field(seq_id, field, default):
    if pd.isna(seq_id):
        return default
    return seq_lookup.get(int(seq_id), {}).get(field, default)

# Flag columns — default -1 (not in a sequence)
for col in ["ra_with_pa","ra_without_pa","pa_without_ra"]:
    df[col] = df["sequence_id"].apply(lambda x, c=col: get_field(x, c, NULL_INT))
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(NULL_INT)

# Timing columns — default -1 (not applicable)
for col in ["pa_to_ra_min","ra_to_ac_min"]:
    df[col] = df["sequence_id"].apply(lambda x, c=col: get_field(x, c, NULL_FLOAT))
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(NULL_FLOAT)

# Other numeric columns
df["sequence_id"]    = pd.to_numeric(df["sequence_id"], errors="coerce").fillna(NULL_INT)
df["city_id"]        = pd.to_numeric(df["city_id"],     errors="coerce").fillna(NULL_INT)
df["lat"]            = pd.to_numeric(df["lat"],          errors="coerce").fillna(0.0)
df["lng"]            = pd.to_numeric(df["lng"],          errors="coerce").fillna(0.0)

df["hour_bucket"]     = (df["timestamp"].dt.hour // 3 * 3).apply(
    lambda h: f"{h:02d}:00-{h+2:02d}:59")
df["day_of_week"]     = df["timestamp"].dt.day_name()
df["day_of_week_num"] = df["timestamp"].dt.dayofweek

# ── VERIFY ────────────────────────────────────
print("\nColumn value check (should show only numbers, no blanks):")
for col in ["sequence_id","ra_with_pa","ra_without_pa","pa_without_ra","pa_to_ra_min","ra_to_ac_min"]:
    unique_vals = sorted(df[col].dropna().unique()[:5].tolist())
    print(f"  {col}: {unique_vals}")

# ── SAVE ──────────────────────────────────────
print("\nSaving files...")

# Use comma as decimal separator to match Israeli locale in Power BI
df.to_csv(OUTPUT_MAIN, index=False, encoding="utf-8-sig", decimal=",")
print(f"  Saved {OUTPUT_MAIN}  ({len(df):,} rows, {len(df.columns)} cols)")

seq_df.to_csv(OUTPUT_SEQUENCES, index=False, encoding="utf-8-sig", decimal=",")
print(f"  Saved {OUTPUT_SEQUENCES}  ({len(seq_df):,} rows)")

print("""
Done. Load both CSVs into Power BI.

POWER QUERY — just set these types, no Replace Values needed:
  sequence_id, city_id, ra_with_pa, ra_without_pa,
  pa_without_ra, has_pa, has_ra, has_ac  →  Whole Number
  pa_to_ra_min, ra_to_ac_min, lat, lng   →  Decimal Number
  date                                   →  Date
  timestamp                              →  Date/Time/Timezone

In DAX measures, filter out 0 sentinel values:
  e.g. FILTER(..., [pa_to_ra_min] > 0)
  For text columns use SUBSTITUTE to handle comma decimals:
  e.g. VALUE(SUBSTITUTE([pa_to_ra_min], ".", ","))
""")