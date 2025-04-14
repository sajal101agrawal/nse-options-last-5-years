# 📈 NSE Options Data Analysis (2020–2025)

This project is built to **analyze NSE (National Stock Exchange) options data** over the past 5 years. It combines historical market data with financial modeling to compute **implied volatility (IV)**, **realized volatility (RV)**, **Black-Scholes Greeks**, **earnings calendar alignment**, and **interest rate integration** for each option.

> 📅 **Data Range:** Apr-2020 to Apr-2025  
> 🛠️ **Primary Use Case:** Quantitative Research, Options Analytics, and AI Training  
> ⚠️ **Disclaimer:** This is for educational and research use only. Follow NSE’s data policies.

---

## 📁 Project Structure

```
.
├── bhavcopy/extracted         # Raw NSE Bhavcopy files
├── earning_dates/             # Earnings dates (JSON format)
├── interest_rates/            # MIFOR interest rate CSVs
├── yahoo_finance/             # Underlying stock prices from Yahoo Finance
├── processed_data/            # Final enriched dataset output
├── download_bhavcopy.py       # Downloader for historical bhavcopies
├── download_yahoo_data.py     # Script to fetch Yahoo historical prices
├── nse_fno_scripts.py         # Script to generate NSE F&O scripts list
├── process_data.py            # Main processor to calculate IV, RV, Greeks, etc.
└── nse_fno_scripts.json       # NSE F&O symbols
```

---

## ✨ Key Features

- 📅 **5 Years of Options Data** with expiry tracking (30d/60d/90d)
- 📈 **Implied Volatility (IV)** via Black-Scholes & bisection method
- 📉 **Realized Volatility (RV)** using **Yang-Zhang model**
- 🧮 **Greeks Calculation**: Delta, Gamma, Theta, Vega, Rho
- 💡 **IV Percentile & Rank** (30-day rolling window)
- 📊 **Interest Rate Fallback Logic** using FBIL MIFOR data
- 🧠 **AI/ML Ready Output**: structured JSON suitable for training or analytics
- 📆 **Earnings Date Mapping** from Sensibull API
- 💾 **Optimized JSON Output** for scalable querying or dashboard integration

---

## ⚙️ Setup & Requirements

**Python Version:** 3.7+  
**Dependencies:**
```bash
pip install pandas numpy requests pytz python-dateutil beautifulsoup4
```

---

## 🚀 How to Use

### 1. Download Bhavcopy Files
```bash
python download_bhavcopy.py
```
> ✅ Recent bhavcopies are sourced from [SAMCO Bhavcopy Archive](https://www.samco.in/bhavcopy-nse-bse-mcx)

---

### 2. Download Underlying Spot Prices (Yahoo Finance)
```bash
python download_yahoo_data.py
```
> Downloads `{symbol}_processed.json` for every script in `yahoo_finance/`

---

### 3. Process & Analyze
```bash
python process_data.py
```
> Merges bhavcopy + interest + earnings + spot prices and computes:
- IV30, IV60, IV90 for CE and PE
- Realized Volatility (Yang-Zhang)
- IV Percentile & Rank (30-day rolling)
- Greeks: Δ, Γ, Θ, ν, ρ
- Upcoming earnings alignment

Saves results in: `processed_data/processed_data.json`

---

## 🧠 Sample Output Format

Path: `processed_data/processed_data.json`

```json
{
  "historical": {
    "scripts": {
      "HDFCBANK": {
        "exchange": "NSE",
        "segment": "Options",
        "09-Apr-2025": {
          "underlying_price": 1765.0,
          "interest_rate": 7.7556,
          "upcoming_earning_date": "19-Apr-2025",
          "expiry_30d": "24-Apr-2025",
          "expiry_60d": "29-May-2025",
          "expiry_90d": "26-Jun-2025",
          "strike_price": 1760.0,
          "rv_yz": 12.0535,
          "ce": {
            "iv_30": 21.40,
            "iv_60": 15.17,
            "iv_90": 19.60,
            "volume": 89185,
            "last_price_30d": 42.4,
            "open": 45.25,
            "high": 47.85,
            "low": 37.25,
            "close": 42.4,
            "ivp": 93.33,
            "ivr": 0.9024,
            "greeks": {
              "delta": 0.56,
              "gamma": 0.0042,
              "theta": -1.01,
              "vega": 1.70,
              "rho": 0.56
            }
          },
          "pe": {
            "iv_30": 21.59,
            "iv_60": 20.89,
            "iv_90": 21.02,
            "volume": 47362,
            "last_price_30d": 31.7,
            "open": 29.5,
            "high": 37.65,
            "low": 28.65,
            "close": 31.7,
            "ivp": 96.67,
            "ivr": 0.9496,
            "greeks": {
              "delta": -0.44,
              "gamma": 0.0042,
              "theta": -0.69,
              "vega": 1.70,
              "rho": -0.48
            }
          }
        }
      }
    }
  }
}
```

---

## 📘 Data Sources

- **NSE Bhavcopy:** [SAMCO Archive](https://www.samco.in/bhavcopy-nse-bse-mcx)
- **Interest Rates:** [FBIL Adjusted MIFOR](https://www.fbil.org.in/#contentA)
- **Earnings Dates:** `https://oxide.sensibull.com/v1/compute/market_stock_events`
- **Underlying Prices:** Yahoo Finance (via `chart` API)

---

## 🔮 Future Enhancements

- 🛰️ **Real-time Option Chain** using broker APIs (e.g., Dhan)
- 🔗 **NoSQL Database Integration** for faster querying
- 📡 **IV Spike Prediction Model** around earnings
- 📺 **React/Django Dashboard** to visualize insights

---

## 🔐 License

Open for non-commercial research and education only. Please review data source terms from NSE, FBIL, Yahoo, Sensibull, and SAMCO.

---

## 👨‍💻 Author

**Sajal Agrawal**  
Founder, Sajal Tech Solutions Private Limited  
🌐 [sajaltech.com](https://sajaltech.com) | ✉️ sajal@sajaltech.com | 📞 +91-8440898969

---

## 🤝 Contributions

Pull requests, forks, and feedback are welcome! Let’s push the frontier of options analytics together 🚀
