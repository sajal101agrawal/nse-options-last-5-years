# 📈 NSE Options Data Analysis (2020–2025)

This project is designed to **analyze NSE (National Stock Exchange) options data** over the past 5 years. It includes features such as historical data collection, implied volatility (IV) and realized volatility (RV) calculations, earnings date tracking, and interest rate integration.

> 📅 **Data Range:** Apr-2020 to Apr-2025  
> 🛠️ **Primary Use Case:** Research, Analysis, and AI-based Options Intelligence  
> ⚠️ **Disclaimer:** For educational and research purposes only. Ensure compliance with NSE’s data usage policies.

---

## 📁 Project Structure

```
.
├── bhavcopy/extracted        # Raw & extracted NSE Bhavcopy files
├── earning_dates/            # Historical earnings date information (JSON)
├── interest_rates/           # MIFOR interest rate data from FBIL
├── yahoo_finance/            # Daily underlying stock prices (5 years)
├── processed_data/           # Final processed and enriched dataset
├── download_bhavcopy.py      # Script to download historical Bhavcopies
├── download_yahoo_data.py    # Script to fetch underlying price data
├── nse_fno_scripts.py        # Generates list of NSE F&O scripts
├── process_data.py           # Main script to compute IV, RV, etc.
├── raw_earning_dates.json    # Sensibull raw earnings data backup
└── nse_fno_scripts.json      # Final JSON of all F&O scripts
```

---

## ✨ Key Features

- ⏳ **5 Years of Options Data** (Bhavcopies from 2020–2025)
- 💰 **Implied Volatility (IV)** using Black-Scholes & bisection method
- 📉 **Realized Volatility (RV)** using the Yang-Zhang model
- 📆 **Earnings Date Analysis** using Sensibull API
- 💸 **Interest Rate Adjustments** using historical MIFOR data
- 📊 **Daily Underlying Prices** from Yahoo Finance
- 🔁 **Percentile & Rank** calculations for IV metrics
- 🧠 **Ready for AI/ML training & analytics platforms**

---

## ⚙️ Setup & Requirements

**Python Version:** 3.x  
**Install Required Packages:**
```bash
pip install pandas numpy requests pytz python-dateutil beautifulsoup4
```

---

## 🚀 Usage

### 1. Download Historical Bhavcopies
```bash
python download_bhavcopy.py
```
> ⚠️ NSE has recently stopped sharing Bhavcopies. Recent data is fetched from [SAMCO Archive](https://www.samco.in/bhavcopy-nse-bse-mcx).

---

### 2. Download Underlying Price Data (Yahoo Finance)
```bash
python download_yahoo_data.py
```
> Downloads `{script}_processed.json` in `yahoo_finance/` with daily prices for each security.

---

### 3. Process Data & Generate Final Output
```bash
python process_data.py
```
> Merges all data sources to compute:
- IV30, IV60, IV90
- RV using Yang-Zhang
- IV Percentile & Rank
- Time to expiry
- Earnings date alignment

Final output: `processed_data/processed_data.json`

---

## 📚 Data Details

### ✅ NSE Bhavcopies (`bhavcopy/`)
Historical files from 11-Apr-2020 to 11-Apr-2025 containing:
```
INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,
OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,
VAL_INLAKH,OPEN_INT,CHG_IN_OI,TIMESTAMP
```

> Recent bhavcopies are fetched from: https://www.samco.in/bhavcopy-nse-bse-mcx

---

### ✅ Interest Rates (`interest_rates/ADJUSTED_MIFOR.csv`)
- Historical MIFOR rates used as the **risk-free interest rate** in IV calculations.
- Source: [FBIL Official Website](https://www.fbil.org.in/#contentA)

---

### ✅ Earnings Dates (`earning_dates/earning_dates.json`)
Fetched via **Sensibull API** with this `curl`:
```
curl --location 'https://oxide.sensibull.com/v1/compute/market_stock_events' \
--header 'content-type: application/json' \
--data '{"from_date":"2015-04-13","to_date":"2025-06-28"}'
```
- Covers results from **25-Apr-2022 to 28-May-2025**
- Stores **upcoming earning date** per stock per day

---

### ✅ Yahoo Finance Prices (`yahoo_finance/{script}_processed.json`)
- Daily closing price of each stock for the last 5 years
- Used as the **underlying price (S)** in Black-Scholes

---

### ✅ NSE F&O Scripts (`nse_fno_scripts.json`)
Generated via `nse_fno_scripts.py`

---

## 🧮 Output Data Format

Output file: `processed_data/processed_data.json`

```json
{
  "historical": {
    "scripts": {
      "HDFCBANK": {
        "exchange": "NSE",
        "segment": "Options",
        "13-Apr-2020": {
            "underlying_price": 895.3499755859375,
            "interest_rate": null,
            "upcoming_earning_date": "15-Apr-2023",
            "expiry_30d": "30-Apr-2020",
            "expiry_60d": "28-May-2020",
            "expiry_90d": "25-Jun-2020",
            "strike_price": 900.0,
            "rv_yz": null,
            "ce": {
              "iv_30": 65.67282076822227,
              "iv_60": 65.6310449117143,
              "iv_90": 81.6354348899034,
              "volume": 10802,
              "last_price": 50.25,
              "close": 50.25,
              "open": 66.7,
              "high": 75.0,
              "low": 46.0,
              "iv_30d_percentile": 100.0,
              "iv_30d_rank": null
            },
            "pe": {
              "iv_30": 65.986026747342,
              "iv_60": 70.54357163582469,
              "iv_90": 76.51776370505534,
              "volume": 5448,
              "last_price": 55.15,
              "close": 55.15,
              "open": 42.0,
              "high": 61.45,
              "low": 39.7,
              "iv_30d_percentile": 100.0,
              "iv_30d_rank": null
            }
         }
      }
    }
  }
}
```

---

## 📦 Future Enhancements

- ✅ **Daily Update Automation** using APIs (e.g., Dhan)
- 📡 Add **real-time NSE option chain integration**
- 🧠 Extend to AI predictions for IV spike before earnings
- 🌐 Deploy a NoSQL DB-backed dashboard using this output

---

## 🔐 Notes

- All files are **locally processed** and can be uploaded to a NoSQL DB for scalable querying.
- Final output (`processed_data.json`) is optimized for daily updates and downstream analytics.

---

## 📜 License

This project is open for educational and non-commercial research. Ensure compliance with data providers' terms (NSE, Yahoo, FBIL, Sensibull, SAMCO).

---

## 👨‍💻 Author

**Sajal Agrawal**  
Founder, Sajal Tech Solutions Private Limited  
🌐 [sajaltech.com](https://sajaltech.com) | ✉️ sajal@sajaltech.com | +91-8440898969

---

## 🙌 Contributions

Feel free to raise issues, suggest enhancements, or contribute improvements via pull requests or forks. Let's make F&O analytics better!