# 📈 NSE Options Data Analysis Toolkit (2020 – 2025)

A one‑stop, **production‑ready pipeline** for *downloading*, *enriching*, *storing*, and *analysing* five years of NSE derivatives data.
It fuses Bhavcopy files, Yahoo spot prices, interest‑rate curves, and earnings calendars, then computes IV, RV, Greeks, IV‑percentile / rank, and streams everything straight into **PostgreSQL** (JSONB) or a flat JSON file for ML workflows.

> 📅 **Coverage** Latest five years from the day you run the scripts
> 🛠️ **Primary use‑cases** Quant research · Options & volatility dashboards · AI model training
> ⚠️ **Disclaimer** Educational/research use only. Respect NSE & data‑provider T\&Cs.

---

## 📁 Project layout

```
.
├── bhavcopy/
│   ├── raw/                    # Downloaded NSE ZIPs
│   └── extracted/              # Daily CSVs
├── earning_dates/              # Earnings-calendar JSON
├── interest_rates/             # FBIL MIFOR CSV
├── yahoo_finance/              # Underlying spot-price JSON
├── processed_data/             # Final merged dataset
├── store_in_db/                # ➜ bulk ETL into Postgres
│   ├── store_s.py              # multi‑threaded ETL driver
│   └── db_util.py              # connection‑pool & batch‑upsert
├── interactive_view/           # Optional HTML/JS plots
├── scripts/
│   ├── download_bhavcopy.py
│   ├── download_yahoo_data.py
│   ├── nse_fno_scripts.py
│   └── utilities.py
├── process_data.py             # stand‑alone JSON pipeline
├── nse_fno_scripts.json
├── top_nse_fno_scripts.json
├── nse_options_formulae.pdf    # maths reference
├── requirements.txt
└── README.md   ← you are here
```

*(macOS users: ignore any auto‑created `__MACOSX/` folders.)*

---

## ✨ Feature highlights

| Category                | What you get                                                                                                                                                                                                       |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Data ingestion**      | • NSE Bhavcopy (zip → CSV) • Yahoo Finance spot prices • FBIL MIFOR daily rates • Earnings calendar (Sensibull‑style JSON)                                                                                         |
| **Analytics engine**    | • **Implied volatility** (30 / 60 / 90 d) via Black‑Scholes + bisection • **Realised vol** with Yang‑Zhang • Full **Greeks** (Δ Γ Θ ν ρ) • Rolling 30‑day **IV percentile & rank**                                 |
| **Index‑aware logic**   | Correct handling of index *weekly* expiries → bins into proper monthly buckets (30/60/90 d).                                                                                                                       |
| **Storage options**     | 1️⃣ Write enriched JSON to `processed_data/processed_data.json`  2️⃣ (*Recommended*) `store_in_db/store_s.py` bulk‑upserts rows into **PostgreSQL** with JSONB columns, automatic retries, and connection pooling. |
| **Speed**               | Multithreaded symbol processing; NumPy releases the GIL → linear scaling on CPUs.                                                                                                                                  |
| **Visualisation hooks** | `interactive_view/` seeds a Plotly/Dash playground; connect directly to the DB or the JSON file.                                                                                                                   |
| **Documentation**       | `nse_options_formulae.pdf` explains every calculation and assumption.                                                                                                                                              |

---

## ⚙️ Quick‑start

### 0.  Environment

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 1.  (Optional) refresh the F\&O script list

```bash
python scripts/nse_fno_scripts.py
```

### 2.  Download raw data

*(Bhavcopies until Apr‑2025 already included; run these only for new dates)*

```bash
python scripts/download_bhavcopy.py       # Bhavcopy ZIPs → CSVs
python scripts/download_yahoo_data.py     # Underlying spot prices
```

### 3‑A.  **JSON‑only** pipeline

```bash
python process_data.py                    # writes processed_data/processed_data.json
```

### 3‑B.  **PostgreSQL** pipeline (preferred)

1. Create a DB and table `option_metrics` (DDL in `store_in_db/schema.sql`).
2. Export PG creds as env‑vars **or** set `PG_DSN`.
3. Run:

   ```bash
   cd store_in_db
   python store_s.py                     # multi‑threaded ETL → Postgres
   ```

*Tunable env‐vars:*

| Var                 | Default | Description                  |
| ------------------- | ------- | ---------------------------- |
| `BATCH_SIZE`        | 1000    | rows per bulk upsert         |
| `PG_SSLMODE`        | require | SSL mode (disable for local) |
| `MAX_WORKERS`       | auto    | threads = `min(8, CPU)`      |
| `STATEMENT_TIMEOUT` | 0       | session timeout (ms)         |

---

## 🧠 Sample JSON snapshot

```jsonc
{
  "historical": {
    "scripts": {
      "NIFTY": {
        "02-May-2025": {
          "underlying_price": 22715.2,
          "interest_rate": 7.78,
          "upcoming_earning_date": null,
          "expiry_30d": "29-May-2025",
          "expiry_60d": "26-Jun-2025",
          "expiry_90d": "31-Jul-2025",
          "strike_price": 22700.0,
          "rv_yz": 11.32,
          "ce": {
            "iv_30": 13.4,
            "iv_60": 14.1,
            "iv_90": 15.3,
            "volume": 181492,
            "ivp": 78.6,
            "ivr": 62.4,
            "greeks": { "delta": 0.52, "gamma": 0.0007 }
          },
          "pe": { … },
          "option_chain": [ … 1200 rows … ]
        }
      }
    }
  }
}
```

---

## 🗄️ Database schema (PostgreSQL)

```sql
CREATE TABLE option_metrics (
  symbol              TEXT    NOT NULL,
  date                DATE    NOT NULL,
  underlying_price    DOUBLE PRECISION,
  interest_rate       DOUBLE PRECISION,
  strike_price        DOUBLE PRECISION,
  expiry_30d          DATE,
  expiry_60d          DATE,
  expiry_90d          DATE,
  upcoming_earning_date DATE,
  rv_yz               DOUBLE PRECISION,
  ce                  JSONB,
  pe                  JSONB,
  option_chain        JSONB,
  extras              JSONB,
  PRIMARY KEY (symbol, date)
);
```

The ETL performs `INSERT … ON CONFLICT DO UPDATE`, so reruns are idempotent.

---

## 🔮 Road‑map

* 🌐 Live chain capture via broker WebSockets (Dhan, Zerodha, FYERS)
* 📈 Real‑time dashboards (Plotly‑Dash & Superset)
* 🤖 Volatility‑surface fitting & Monte‑Carlo pricers
* 🏃‍♂️ Streaming pipeline with Apache Airflow + TimescaleDB

---

## 📚 Data sources

| Source                       | Usage                          |
| ---------------------------- | ------------------------------ |
| **NSE Bhavcopy**             | Official market snapshot (EOD) |
| **Yahoo Finance**            | Spot prices for IV calc        |
| **FBIL MIFOR**               | Daily INR risk‑free proxy      |
| **Sensibull API / Raw JSON** | Corporate actions & earnings   |

---

## 🔐 Licence

Non‑commercial, research & educational use.
You **must** comply with upstream providers (NSE, FBIL, Yahoo, etc.) when redistributing or displaying data.

---

## 👤 Author

**Sajal Agrawal** — Founder, Sajal Tech Solutions Pvt Ltd
🌐 [sajaltech.com](https://sajaltech.com) | ✉ [sajal@sajaltech.com](mailto:sajal@sajaltech.com)

---

## 🤝 Contributing

Bug‑reports, pull‑requests, and feature discussions are warmly welcome.
Let’s keep pushing the frontier of open‑source options analytics! 🚀
