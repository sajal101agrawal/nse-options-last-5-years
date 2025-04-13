import os
import pandas as pd
import json
import glob
import math
from datetime import datetime, timedelta
import numpy as np
import pytz
from bisect import bisect_right

#############################################################################
#                       BLACK-SCHOLES & IV CALCULATIONS                     #
#############################################################################

def black_scholes_price(S, K, T, r, sigma, is_call=True, q=0.0):
    """
    Computes the theoretical option price using the Black-Scholes-Merton formula.
    S  : Underlying price
    K  : Strike price
    T  : Time to maturity (in years)
    r  : Risk-free interest rate (annualized, decimal form)
    sigma : Volatility (annualized, decimal form)
    is_call : True for Call, False for Put
    q  : Continuous dividend yield (decimal)
    """
    if T <= 0:
        # No time => option = intrinsic value
        return max(0.0, S - K) if is_call else max(0.0, K - S)
    
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    # Standard normal CDF
    def phi(x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
    
    e_neg_qT = math.exp(-q * T)
    e_neg_rT = math.exp(-r * T)
    
    if is_call:
        return (S * e_neg_qT * phi(d1)) - (K * e_neg_rT * phi(d2))
    else:
        return (K * e_neg_rT * phi(-d2)) - (S * e_neg_qT * phi(-d1))

def implied_volatility_bisection(
    market_price, S, K, T, r, is_call=True, q=0.0,
    lower_bound=1e-9, upper_bound=5.0, tol=1e-8, max_iter=500
):
    """
    Finds implied volatility using a simple bisection method on Black-Scholes price.
    market_price: observed option price (premium)
    S  : Underlying price
    K  : Strike price
    T  : Time to expiry (years)
    r  : Risk-free rate (decimal)
    is_call : True for Call, False for Put
    q  : Dividend yield (decimal)
    """
    intrinsic = max(0.0, (S - K) if is_call else (K - S))
    if T <= 0 or market_price <= intrinsic:
        return 0.0
    
    for _ in range(max_iter):
        mid_vol = 0.5 * (lower_bound + upper_bound)
        price_mid = black_scholes_price(S, K, T, r, mid_vol, is_call, q)
        
        if abs(price_mid - market_price) < tol:
            return mid_vol
        
        if price_mid > market_price:
            upper_bound = mid_vol
        else:
            lower_bound = mid_vol
    
    return 0.5 * (lower_bound + upper_bound)

#############################################################################
#                  YANG-ZHANG REALIZED VOLATILITY (DAILY)                   #
#############################################################################

def yang_zhang_volatility(ohlc_df, trading_periods=252):
    """
    Computes the Yang-Zhang volatility for a given set of daily OHLC data.
    Typically you do this over a window (e.g. last 20 or 30 days).
    ohlc_df columns required: 'Open', 'High', 'Low', 'Close'.
    trading_periods is the annualization factor (252 for trading days).
    """
    n = len(ohlc_df)
    if n < 2:
        # Not enough data for the YZ formula
        return float('nan')

    # Convert to logs
    open_prices = np.log(ohlc_df['Open'].values)
    close_prices = np.log(ohlc_df['Close'].values)
    high_prices = np.log(ohlc_df['High'].values)
    low_prices  = np.log(ohlc_df['Low'].values)
    
    # Close-to-close variance
    cc_returns = np.diff(close_prices)
    if len(cc_returns) < 1:
        return float('nan')
    sigma_cc_sq = np.var(cc_returns, ddof=1)

    # Open-to-open variance
    oo_returns = np.diff(open_prices)
    sigma_oo_sq = np.var(oo_returns, ddof=1) if len(oo_returns) > 1 else 0.0
    
    # Overnight returns
    overnight = open_prices[1:] - close_prices[:-1]
    if len(overnight) < 1:
        return float('nan')
    mean_overnight = np.mean(overnight)
    sigma_on_sq = np.sum((overnight - mean_overnight)**2) / (n - 1)
    
    # Weighted factor k
    k = 0.34 / (1.34 + (n+1)/(n-1))
    
    # YZ variance
    yz_variance = sigma_cc_sq + k*sigma_on_sq - (1 - k)*sigma_oo_sq
    
    # If negative or NaN, bail
    if not np.isfinite(yz_variance) or yz_variance <= 0:
        return float('nan')
    
    # Otherwise, annualize
    yz_vol = math.sqrt(yz_variance * trading_periods)
    return yz_vol

def compute_yz_rolling_vol(df, window=20, trading_periods=252):
    """
    Given a daily OHLC DataFrame (with columns Date,Open,High,Low,Close),
    compute a rolling Yang-Zhang realized vol over the specified window.
    Returns a pd.Series with the same date index, each row is the YZ vol
    computed over the prior `window` days (including current day).
    """
    # Sort by date just in case
    df = df.sort_values('Date')
    df.set_index('Date', inplace=True)
    
    # We'll define a rolling apply:
    def yz_func(subdf):
        return yang_zhang_volatility(subdf, trading_periods)
    
    # Rolling window apply
    # If df is large, this can be slow. We can do a .rolling(window).apply(...) if we pass a function
    # that can handle arrays. But we need O/H/L/C. We'll just do a group apply for demonstration:
    # Alternatively, use a custom approach or 'rolling' trick.
    
    # We'll do a simple approach using a loop to illustrate:
    yz_values = []
    dates = df.index
    for i in range(len(dates)):
        # window from i-window+1 to i
        start_idx = max(0, i - window + 1)
        subdf = df.iloc[start_idx:(i+1)]
        rv = yang_zhang_volatility(subdf, trading_periods)
        yz_values.append(rv)
    
    result_series = pd.Series(yz_values, index=dates, name='YZ_Vol')
    return result_series

#############################################################################
#               INTEREST-RATE FALLBACK (NEAREST EARLIER DATE)               #
#############################################################################

def get_rate_with_fallback(date_obj, daily_interest_map):
    """
    If an exact match for date_obj is not present, pick the nearest earlier date
    in daily_interest_map. If date_obj is before the earliest known date, return None.
    
    daily_interest_map is {python_date: rate}.
    """
    # Sort all available dates
    all_dates = sorted(daily_interest_map.keys())
    pos = bisect_right(all_dates, date_obj)
    if pos == 0:
        # date_obj < all_dates[0], no fallback
        return None
    else:
        # Fallback date is all_dates[pos - 1]
        fallback_date = all_dates[pos - 1]
        return daily_interest_map[fallback_date]

#############################################################################
#                 PICK STRIKE NEAREST UNDERLYING (CE/PE)                    #
#############################################################################

def pick_strike_nearest_underlying(underlying, options_data):
    """
    Among all strikes in options_data, pick the single strike that is closest
    to the 'underlying' price and is available with both CE & PE for the
    nearest, next-nearest, and next-to-next-nearest expiry.
    Returns:
        ce_row_30d, ce_row_60d, ce_row_90d,
        pe_row_30d, pe_row_60d, pe_row_90d,
        chosen_strike
    """
    candidates = []

    # Ensure datetime format
    options_data = options_data.copy()
    options_data['EXPIRY_DT'] = pd.to_datetime(options_data['EXPIRY_DT'], errors='coerce')
    if options_data['EXPIRY_DT'].isnull().all():
        return None

    # Get first 3 unique expiry dates
    unique_expiries = sorted(options_data['EXPIRY_DT'].dropna().unique())
    if len(unique_expiries) < 3:
        return None

    expiry_30d = unique_expiries[0]
    expiry_60d = unique_expiries[1]
    expiry_90d = unique_expiries[2]

    data_30d = options_data[options_data['EXPIRY_DT'] == expiry_30d]
    data_60d = options_data[options_data['EXPIRY_DT'] == expiry_60d]
    data_90d = options_data[options_data['EXPIRY_DT'] == expiry_90d]

    # Try to find a strike that exists in all 3 expiries and has both CE and PE
    common_strikes = set(data_30d['STRIKE_PR'].unique()) & \
                     set(data_60d['STRIKE_PR'].unique()) & \
                     set(data_90d['STRIKE_PR'].unique())

    for strike_price in common_strikes:
        strike_price = float(strike_price)

        def get_rows(data, strike):
            sub = data[data['STRIKE_PR'] == strike]
            ce = sub[sub['OPTION_TYP'] == 'CE']
            pe = sub[sub['OPTION_TYP'] == 'PE']
            return ce.iloc[0] if not ce.empty else None, pe.iloc[0] if not pe.empty else None

        ce_30d, pe_30d = get_rows(data_30d, strike_price)
        ce_60d, pe_60d = get_rows(data_60d, strike_price)
        ce_90d, pe_90d = get_rows(data_90d, strike_price)

        diff = abs(strike_price - underlying)
        candidates.append((diff, strike_price, ce_30d, pe_30d, ce_60d, pe_60d, ce_90d, pe_90d))

    if not candidates:
        return None

    # Sort by closest to underlying
    candidates.sort(key=lambda x: x[0])
    _, chosen_strike, ce_30d, pe_30d, ce_60d, pe_60d, ce_90d, pe_90d = candidates[0]

    return ce_30d, ce_60d, ce_90d, pe_30d, pe_60d, pe_90d, chosen_strike



#############################################################################
#                          TIME-TO-EXPIRY FUNCTION                          #
#############################################################################

def get_time_to_expiry_in_years(expiry_str):
    """
    Converts an expiry date string like '25-Apr-2025' into the fractional
    number of years until expiry from the current time in India.
    """
    india_tz = pytz.timezone("Asia/Kolkata")
    expiry_naive = datetime.strptime(expiry_str, "%d-%b-%Y")
    expiry_date = india_tz.localize(expiry_naive)
    
    now_india = datetime.now(india_tz)
    time_diff = expiry_date - now_india
    
    days_to_expiry = time_diff.days + time_diff.seconds / 86400.0
    days_to_expiry = max(0, days_to_expiry)  # clamp negative to zero
    
    # Approx with 300 days a year:
    return days_to_expiry / 300.0


#############################################################################
#                            MAIN PROCESSING                                 #
#############################################################################

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bhavcopy_dir = os.path.join(base_dir, 'bhavcopy', 'extracted')
    processed_dir = os.path.join(base_dir, 'processed_data')
    os.makedirs(processed_dir, exist_ok=True)
    
    

    # ----------------------------------------------------------------------
    # 1) LOAD F&O SCRIPTS
    # ----------------------------------------------------------------------
    with open(os.path.join(base_dir, 'nse_fno_scripts.json'), 'r') as f:
        fno_scripts = json.load(f)

    symbols = ([itm["symbol"] for itm in fno_scripts['index_futures']] + 
               [itm["symbol"] for itm in fno_scripts['individual_securities']])
    print(f"Processing data for {len(symbols)} symbols: {', '.join(symbols)}")
    
    
    # ----------------------------------------------------------------------
    #  LOAD UNDERLYING SPOT PRICES FROM YAHOO
    # ----------------------------------------------------------------------
    yahoo_dir = os.path.join(base_dir, "yahoo_finance")
    spot_price_map = {}
    for symbol in symbols:
        json_path = os.path.join(yahoo_dir, f"{symbol}_processed.json")
        if not os.path.exists(json_path):
            continue
        with open(json_path, 'r') as yf:
            data = json.load(yf)
            try:
                timestamps = data['historical']['scripts'][symbol]['timestamps']
                spot_price_map[symbol] = {
                    date: ts_data['underlying_price']
                    for date, ts_data in timestamps.items()
                    if 'underlying_price' in ts_data
                }
            except KeyError:
                continue
            

    # ----------------------------------------------------------------------
    # 2) LOAD BHAVCOPY FILES
    # ----------------------------------------------------------------------
    bhavcopy_files = glob.glob(os.path.join(bhavcopy_dir, '*.csv'))
    print(f"Found {len(bhavcopy_files)} bhavcopy files to process")

    # ----------------------------------------------------------------------
    # 3) LOAD INTEREST RATES FROM CSV WITH FALLBACK
    # ----------------------------------------------------------------------
    interest_rates_csv = os.path.join(base_dir, "interest_rates", "ADJUSTED_MIFOR.csv")
    # If your CSV has no extra lines, remove skiprows=2
    ir_df = pd.read_csv(interest_rates_csv, skiprows=2)
    ir_df["Date_parsed"] = pd.to_datetime(ir_df["Date"], format="%d %b %Y", errors='coerce')
    
    daily_interest_map = {}
    for dt, sub_df in ir_df.groupby("Date_parsed"):
        # pick the first row for that date
        first_row = sub_df.iloc[0]
        daily_interest_map[dt.date()] = float(first_row["FBIL ADJUSTED MIFOR(%)"])

    # ----------------------------------------------------------------------
    # 4) SET UP RESULT STRUCTURE
    # ----------------------------------------------------------------------
    result = {"historical": {"scripts": {}}}
    for symbol in symbols:
        result["historical"]["scripts"][symbol] = {
            "exchange": "NSE",
            "segment": "Options",
            "timestamps": {}
        }

    # ----------------------------------------------------------------------
    # 5) PROCESS BHAVCOPY FILES
    # ----------------------------------------------------------------------
    # For realized vol, we need daily OHLC over time. Let's store it for each symbol
    # so we can do a rolling YZ volatility. We'll do a dict: symbol -> DataFrame of daily bars
    # Then we'll compute YZ at the end.
    daily_ohlc_map = {sym: [] for sym in symbols}

    for file_path in bhavcopy_files:
        try:
            filename = os.path.basename(file_path)
            date_str = filename[2:-8]  # e.g. '13APR2025'
            date_obj = datetime.strptime(date_str, '%d%b%Y')
            print(f"Processing {filename} => {date_obj.strftime('%Y-%m-%d')}")

            # Get interest rate with fallback
            fallback_rate = get_rate_with_fallback(date_obj.date(), daily_interest_map)
            
            df = pd.read_csv(file_path)

            # We gather daily "Open,High,Low,Close" for each symbol from the FUT or from something else
            # Typically you'd use FUT's open/high/low/close or the underlying's actual equity data
            # For demonstration, let's take the nearest future's open/high/low/close as "daily" bars
            # for the symbol. If you have separate equity data, you'd use that instead.
            
            for symbol in symbols:
                symbol_data = df[df['SYMBOL'] == symbol].copy()
                if symbol_data.empty:
                    continue

                # ---------- FUT data for daily OHLC ----------
                futures_data = symbol_data[
                    symbol_data['INSTRUMENT'].isin(['FUTSTK','FUTIDX'])
                ].copy()
                if futures_data.empty:
                    continue

                # Pick nearest expiry
                futures_data['EXPIRY_DT'] = pd.to_datetime(
                    futures_data['EXPIRY_DT'], format='%d-%b-%Y', errors='coerce'
                ).fillna(datetime(2100,1,1))
                futures_data = futures_data.sort_values(by='EXPIRY_DT')

                
                fut_row = futures_data.iloc[0]
                
                # We'll store the FUT's open/high/low/close as daily bar
                day_ohlc = {
                    "Date": date_obj,
                    "Open": float(fut_row['OPEN']),
                    "High": float(fut_row['HIGH']),
                    "Low":  float(fut_row['LOW']),
                    "Close":float(fut_row['CLOSE'])
                }
                daily_ohlc_map[symbol].append(day_ohlc)
                                
                
                # Ensure we have an entry in result for that date
                date_key = date_obj.strftime('%d-%b-%Y')
                
                spot_price = spot_price_map.get(symbol, {}).get(date_key, None)
                
                
                if date_key not in result["historical"]["scripts"][symbol]["timestamps"]:
                    result["historical"]["scripts"][symbol]["timestamps"][date_key] = {
                        "underlying_price": spot_price, 
                        "interest_rate": fallback_rate,
                        "upcoming_earning_date": None,
                        "expiry_30d": None,
                        "expiry_60d": None,
                        "expiry_90d": None
                    }

                # Gather all future expiries for the day => store up to 3
                all_fut_expiries = futures_data['EXPIRY_DT'].dropna().unique()
                all_fut_expiries_sorted = sorted(all_fut_expiries)
                
                if len(all_fut_expiries_sorted) >= 1:
                    result["historical"]["scripts"][symbol]["timestamps"][date_key]["expiry_30d"] = all_fut_expiries_sorted[0].strftime('%d-%b-%Y')
                if len(all_fut_expiries_sorted) >= 2:
                    result["historical"]["scripts"][symbol]["timestamps"][date_key]["expiry_60d"] = all_fut_expiries_sorted[1].strftime('%d-%b-%Y')
                if len(all_fut_expiries_sorted) >= 3:
                    result["historical"]["scripts"][symbol]["timestamps"][date_key]["expiry_90d"] = all_fut_expiries_sorted[2].strftime('%d-%b-%Y')

                # ---------- Options data: pick the strike nearest to underlying ----------
                options_data = symbol_data[
                    symbol_data['INSTRUMENT'].isin(['OPTSTK','OPTIDX'])
                ].copy()
                if options_data.empty:
                    continue

                options_data['EXPIRY_DT'] = pd.to_datetime(
                    options_data['EXPIRY_DT'], format='%d-%b-%Y', errors='coerce'
                )


                eq_data = symbol_data[symbol_data['INSTRUMENT'] == 'EQ']                
                    
                chosen = pick_strike_nearest_underlying(spot_price, options_data)
                # chosen = pick_strike_nearest_underlying(float(fut_row['CLOSE']), options_data)
                
                if not chosen:
                    # No valid single strike
                    continue
                ce_row_30d, ce_row_60d, ce_row_90d, pe_row_30d, pe_row_60d, pe_row_90d, chosen_strike = chosen

                # Time to expiry (from CE row)
                opt_expiry = ce_row_30d['EXPIRY_DT']
                if pd.isnull(opt_expiry):
                    # T = 0.0
                    T_30 = 0.0
                    T_60 = 0.0
                    T_90 = 0.0
                else:
                    T_30 = get_time_to_expiry_in_years(all_fut_expiries_sorted[0].strftime('%d-%b-%Y'))
                    T_60 = get_time_to_expiry_in_years(all_fut_expiries_sorted[1].strftime('%d-%b-%Y'))
                    T_90 = get_time_to_expiry_in_years(all_fut_expiries_sorted[2].strftime('%d-%b-%Y'))
                
                    # T = get_time_to_expiry_in_years(opt_expiry.strftime('%d-%b-%Y'))
                
                r_decimal = (fallback_rate / 100.0) if fallback_rate else 0.0
                
                
                ce_30d_price = float(ce_row_30d['SETTLE_PR'])
                ce_60d_price = float(ce_row_60d['SETTLE_PR'])
                ce_90d_price = float(ce_row_90d['SETTLE_PR'])
                pe_30d_price = float(pe_row_30d['SETTLE_PR'])
                pe_60d_price = float(pe_row_60d['SETTLE_PR'])
                pe_90d_price = float(pe_row_90d['SETTLE_PR'])

                ce_iv_30 = implied_volatility_bisection(
                    market_price=ce_30d_price,
                    S=float(spot_price),
                    K=float(chosen_strike),
                    T=T_30,
                    r=r_decimal,
                    is_call=True
                ) * 100.0
                pe_iv_30 = implied_volatility_bisection(
                    market_price=pe_30d_price,
                    S=float(spot_price),
                    K=float(chosen_strike),
                    T=T_30,
                    r=r_decimal,
                    is_call=False
                ) * 100.0
                
                ce_iv_60 = implied_volatility_bisection(
                    market_price=ce_60d_price,
                    S=float(spot_price),
                    K=float(chosen_strike),
                    T=T_60,
                    r=r_decimal,
                    is_call=True
                ) * 100.0
                
                pe_iv_60 = implied_volatility_bisection(
                    market_price=pe_60d_price,
                    S=float(spot_price),
                    K=float(chosen_strike),
                    T=T_60,
                    r=r_decimal,
                    is_call=False
                ) * 100.0
                
                ce_iv_90 = implied_volatility_bisection(
                    market_price=ce_90d_price,
                    S=float(spot_price),
                    K=float(chosen_strike),
                    T=T_90,
                    r=r_decimal,
                    is_call=True
                ) * 100.0
                
                pe_iv_90 = implied_volatility_bisection(
                    market_price=pe_90d_price,
                    S=float(spot_price),
                    K=float(chosen_strike),
                    T=T_90,
                    r=r_decimal,
                    is_call=False
                ) * 100.0

                # Volume => here we store 'CONTRACTS'. If you want total shares, multiply by lot size or use a different column
                ce_volume = int(options_data[options_data['OPTION_TYP'] == 'CE']['CONTRACTS'].sum())
                pe_volume = int(options_data[options_data['OPTION_TYP'] == 'PE']['CONTRACTS'].sum())


                # Merge data
                result["historical"]["scripts"][symbol]["timestamps"][date_key].update({
                    "strike_price": float(chosen_strike),
                    "rv_yz": None,
                    "ce": {
                        "iv_30": ce_iv_30,
                        "iv_60": ce_iv_60,
                        "iv_90": ce_iv_90,
                        # "open_interest": int(ce_row['OPEN_INT']),
                        "volume": ce_volume,
                        "last_price": ce_30d_price,
                        "close": float(ce_row_30d['CLOSE']),
                        "open": float(ce_row_30d['OPEN']),
                        "high": float(ce_row_30d['HIGH']),
                        "low": float(ce_row_30d['LOW']),
                        "iv_30d_percentile": None,
                        "iv_30d_rank": None,
                    },
                    "pe": {
                        "iv_30": pe_iv_30,
                        "iv_60": pe_iv_60,
                        "iv_90": pe_iv_90,
                        # "open_interest": int(pe_row['OPEN_INT']),
                        "volume": pe_volume,
                        "last_price": pe_30d_price,
                        "close": float(pe_row_30d['CLOSE']),
                        "open": float(pe_row_30d['OPEN']),
                        "high": float(pe_row_30d['HIGH']),
                        "low": float(pe_row_30d['LOW']),
                        "iv_30d_percentile": None,
                        "iv_30d_rank": None,
                    }
                })
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")

    # ----------------------------------------------------------------------
    # 6) AFTER LOADING: Compute Rolling Yang-Zhang Realized Vol for each symbol
    # ----------------------------------------------------------------------
    # We'll do a 20-day rolling approach, for example. Then store each day's yz in the result.
    
    window = 20  # or 30, or as desired
    for symbol in symbols:
        # Build a DF from the daily_ohlc_map
        rows = daily_ohlc_map[symbol]
        if not rows:
            continue
        ohlc_df = pd.DataFrame(rows)  # columns = Date,Open,High,Low,Close
        # compute rolling yz
        yz_series = compute_yz_rolling_vol(ohlc_df, window=window, trading_periods=252)
        
        # yz_series is indexed by date. We'll match each date to the relevant date_str in result
        for date_idx, yz_val in yz_series.items():
            date_key = date_idx.strftime('%d-%b-%Y')
            if date_key in result["historical"]["scripts"][symbol]["timestamps"]:
                if np.isfinite(yz_val):
                    result["historical"]["scripts"][symbol]["timestamps"][date_key]["rv_yz"] = yz_val
                else:
                    # If YZ is NaN or inf, store None
                    result["historical"]["scripts"][symbol]["timestamps"][date_key]["rv_yz"] = None
            # else no entry for that date => skip
            
    
       # ----------------------------------------------------------------------
    # 7) COMPUTE 30-DAY IV PERCENTILE & RANK FOR CE & PE
    # ----------------------------------------------------------------------
    def compute_rolling_iv_percentile_and_rank(iv_df, window_size=30):
        """
        Given a DataFrame with columns [Date, iv_30], compute rolling
        30-day percentile & rank, return them as new columns:
        iv_30d_percentile, iv_30d_rank.
        """
        iv_30d_percentiles = []
        iv_30d_ranks = []
        vals = iv_df['iv_30'].values
        
        for i in range(len(vals)):
            start = max(0, i - window_size + 1)
            sub = vals[start:(i+1)]
            current_val = vals[i]
            if len(sub) == 0 or not np.isfinite(current_val):
                iv_30d_percentiles.append(None)
                iv_30d_ranks.append(None)
                continue
            
            # 1) Percentile
            count_le = np.sum(sub <= current_val)
            percentile = count_le / len(sub)
            
            # 2) Rank (min-max)
            sub_min, sub_max = np.min(sub), np.max(sub)
            if sub_max > sub_min:
                rank = (current_val - sub_min) / (sub_max - sub_min)
            else:
                rank = None
            
            iv_30d_percentiles.append(percentile)
            iv_30d_ranks.append(rank)
        
        iv_df['iv_30d_percentile'] = iv_30d_percentiles
        iv_df['iv_30d_rank'] = iv_30d_ranks
        return iv_df

    # For each symbol, build separate time-series for CE and PE iv_30,
    # then compute rolling stats:
    for symbol in symbols:
        time_map = result["historical"]["scripts"][symbol]["timestamps"]
        if not time_map:
            continue
        
        # 7a) CE
        ce_rows = []
        for d_str, rec in time_map.items():
            ce_iv_30 = rec["ce"]["iv_30"]
            if ce_iv_30 is not None:
                try:
                    dt_obj = datetime.strptime(d_str, "%d-%b-%Y")
                    ce_rows.append({"Date": dt_obj, "iv_30": ce_iv_30})
                except:
                    pass
        if ce_rows:
            df_ce = pd.DataFrame(ce_rows).dropna(subset=["iv_30"])
            df_ce.sort_values("Date", inplace=True)
            df_ce.reset_index(drop=True, inplace=True)
            df_ce = compute_rolling_iv_percentile_and_rank(df_ce, window_size=30)
            
            # Store back
            for i in range(len(df_ce)):
                dt_obj = df_ce.loc[i, "Date"]
                d_str = dt_obj.strftime("%d-%b-%Y")
                pctl = df_ce.loc[i, "iv_30d_percentile"]
                rnk = df_ce.loc[i, "iv_30d_rank"]
                # Convert percentile to % if desired
                time_map[d_str]["ce"]["iv_30d_percentile"] = (pctl * 100.0) if pctl is not None else None
                time_map[d_str]["ce"]["iv_30d_rank"] = rnk  # 0-1 scale

        # 7b) PE
        pe_rows = []
        for d_str, rec in time_map.items():
            pe_iv_30 = rec["pe"]["iv_30"]
            if pe_iv_30 is not None:
                try:
                    dt_obj = datetime.strptime(d_str, "%d-%b-%Y")
                    pe_rows.append({"Date": dt_obj, "iv_30": pe_iv_30})
                except:
                    pass
        if pe_rows:
            df_pe = pd.DataFrame(pe_rows).dropna(subset=["iv_30"])
            df_pe.sort_values("Date", inplace=True)
            df_pe.reset_index(drop=True, inplace=True)
            df_pe = compute_rolling_iv_percentile_and_rank(df_pe, window_size=30)
            
            # Store back
            for i in range(len(df_pe)):
                dt_obj = df_pe.loc[i, "Date"]
                d_str = dt_obj.strftime("%d-%b-%Y")
                pctl = df_pe.loc[i, "iv_30d_percentile"]
                rnk = df_pe.loc[i, "iv_30d_rank"]
                time_map[d_str]["pe"]["iv_30d_percentile"] = (pctl * 100.0) if pctl is not None else None
                time_map[d_str]["pe"]["iv_30d_rank"] = rnk
                
                
    # SORT TIMESTAMPS BY DATE AND REPLACE NaN/Inf => None
    for symbol in symbols:
        ts_map = result["historical"]["scripts"][symbol]["timestamps"]
        if not ts_map:
            continue

        # Convert dict to list of (datetime, date_str, data)
        temp_list = []
        for d_str, rec_data in ts_map.items():
            try:
                dt_obj = datetime.strptime(d_str, "%d-%b-%Y")
            except:
                # If parsing fails, skip
                continue
            temp_list.append((dt_obj, d_str, rec_data))
        
        # Sort by the dt_obj
        temp_list.sort(key=lambda x: x[0])

        # Rebuild the dictionary in sorted order
        sorted_dict = {}
        for dt_obj, d_str, rec_data in temp_list:
            sorted_dict[d_str] = rec_data

        # Replace old map with sorted map
        result["historical"]["scripts"][symbol]["timestamps"] = sorted_dict

    # Function to recursively replace NaN/Inf
    def replace_nan_inf(obj):
        if isinstance(obj, dict):
            for k in list(obj.keys()):
                obj[k] = replace_nan_inf(obj[k])
            return obj
        elif isinstance(obj, list):
            for i in range(len(obj)):
                obj[i] = replace_nan_inf(obj[i])
            return obj
        elif isinstance(obj, float):
            if not np.isfinite(obj):
                return None
            else:
                return obj
        else:
            return obj

    # Apply NaN/Inf replacement
    replace_nan_inf(result)
    
    
    
    # ----------------------------------------------------------------------
    # ADD UPCOMING EARNINGS DATES FROM earnings_dates.json
    # ----------------------------------------------------------------------

    earnings_file = os.path.join(base_dir, "earning_dates", "earning_dates.json")
    if os.path.exists(earnings_file):
        with open(earnings_file, "r") as f:
            earnings_data = json.load(f)
            
        earnings_map = {}
        
        for item in earnings_data:
            if item.get("event_type") == "stock_results" and "trading_symbol" in item and "date" in item:
                try:
                    symbol = item["trading_symbol"]
                    dt = datetime.strptime(item["date"], "%Y-%m-%d")
                    earnings_map.setdefault(symbol, []).append(dt)
                except Exception as e:
                    print(f"[EARNINGS] Failed to parse date for {item.get('trading_symbol')}: {e}")

        for symbol in symbols:

            earnings_dates = earnings_map[symbol]
            earnings_dt_objs = sorted(earnings_dates)

            time_map = result["historical"]["scripts"][symbol]["timestamps"]
            for d_str in time_map:
                try:
                    current_dt = datetime.strptime(d_str, "%d-%b-%Y")
                    future_dates = [dt for dt in earnings_dt_objs if dt > current_dt]
                    if future_dates:
                        upcoming = future_dates[0].strftime("%d-%b-%Y")
                        time_map[d_str]["upcoming_earning_date"] = upcoming
                except Exception as e:
                    print(f"[EARNINGS] Error for {symbol} at {d_str}: {e}")
                    
                
    # ----------------------------------------------------------------------
    #  SAVE PROCESSED DATA
    # ----------------------------------------------------------------------
    output_file = os.path.join(processed_dir, 'processed_data.json')
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)

    print(f"Processed data saved to {output_file}")

    # ----------------------------------------------------------------------
    # 8) SUMMARY
    # ----------------------------------------------------------------------
    total_timestamps = 0
    for symbol in symbols:
        timestamps = len(result["historical"]["scripts"][symbol]["timestamps"])
        total_timestamps += timestamps
        print(f"{symbol}: {timestamps} days of data")
    print(f"Total: {total_timestamps} data points across {len(symbols)} symbols")

if __name__ == "__main__":
    main()
