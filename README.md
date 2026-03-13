# 🚨 Red Alert Israel — Live Threat Intelligence Dashboard

A Python + Power BI pipeline that pulls live data from the Israeli Home Front Command alert system, computes alert sequences, and visualises threat patterns across cities and time periods.

Built during the escalation of March 2026.

---

## 📊 Dashboard Preview

> *(Add your Power BI screenshot here)*

---

## 🔍 What It Tracks

| Term | Meaning |
|------|---------|
| **RA** | Red Alert — active siren |
| **PA** | Pre-Alert — advance news flash warning before siren |
| **AC** | All Clear — end of threat, shelter release |
| **PA→RA** | Pre-Alert that escalated to a Red Alert within 15 min |

---

## 📈 Metrics Computed

- Total Red Alerts
- % RAs without a prior PA (no warning)
- % PAs without a following RA (false alarm)
- % PAs that escalated to RA
- Average time PA → RA (minutes)
- Average time RA → AC / shelter duration (minutes)
- Min / Avg / Max shelter time
- Average time between RA events (hours)
- Average RAs per day
- Breakdown by alert type, origin, city, time of day, day of week

---

## 🗂️ Project Structure

```
RedAlertEDA/
│
├── redalert_to_powerbi.py      # Step 1: Pull data from API → redalert_data.csv
├── redalert_enrich.py          # Step 2: Compute sequences → enriched CSVs
│
├── redalert_data.csv           # Raw alert data (gitignored)
├── redalert_data_enriched.csv  # Enriched main table (gitignored)
├── redalert_sequences.csv      # Sequence-level metrics (gitignored)
│
└── README.md
```

---

## ⚙️ Setup

### 1. Prerequisites

```bash
pip install requests pandas
```

### 2. Get an API Key

Request a free API key from [redalert.orielhaim.com](https://redalert.orielhaim.com).

### 3. Configure

Open `redalert_to_powerbi.py` and set your key:

```python
API_KEY = "your-api-key-here"
```

Optionally adjust the date range (default: 28/02/2026 → now):

```python
START_DATE = "2026-02-28T00:00:00Z"
END_DATE   = None   # None = up to now
```

### 4. Run

```bash
# Step 1 — Pull latest data from API
python redalert_to_powerbi.py

# Step 2 — Compute sequences and enrich
python redalert_enrich.py
```

This produces three CSV files ready to load into Power BI.

---

## 📊 Power BI Setup

### Load Data
1. **Home → Get Data → Text/CSV** → load `redalert_data_enriched.csv`
2. Repeat for `redalert_sequences.csv`

### Power Query — Set Column Types

**redalert_data_enriched:**

| Column | Type |
|--------|------|
| `alert_id`, `sequence_id`, `city_id` | Whole Number |
| `ra_with_pa`, `ra_without_pa`, `pa_without_ra` | Whole Number |
| `pa_to_ra_min`, `ra_to_ac_min`, `lat`, `lng` | Decimal Number |
| `date` | Date |
| `timestamp` | Date/Time/Timezone |

**redalert_sequences:**

| Column | Type |
|--------|------|
| `sequence_id` | Whole Number |
| `ra_with_pa`, `ra_without_pa`, `pa_without_ra`, `has_pa`, `has_ra`, `has_ac` | Whole Number |
| `pa_to_ra_min`, `ra_to_ac_min`, `pa_to_ra_sec`, `ra_to_ac_sec` | Decimal Number |
| `pa_time`, `first_ra_time`, `ac_time` | Date/Time/Timezone |

### Relationship
- Model view → link `redalert_data_enriched[sequence_id]` → `redalert_sequences[sequence_id]`
- Many to One, Single direction

### Daily Refresh
Just re-run both scripts and click **Home → Refresh** in Power BI. All measures and visuals update automatically.

---

## 🔢 Key DAX Notes

- Numeric sentinel value is `0` — filter with `> 0` to exclude non-applicable rows
- Decimal separator is `,` (Israeli locale) — use `VALUE(SUBSTITUTE([col], ".", ","))` if needed
- All flag columns (`ra_with_pa` etc.) are `0` or `1`

---

## 📡 Data Source

Data is sourced from [redalert.orielhaim.com](https://redalert.orielhaim.com) which aggregates official Israeli Home Front Command (Pikud HaOref) alerts in real time.

---

## 🙏 Stay Safe

Built in Israel, March 2026.
