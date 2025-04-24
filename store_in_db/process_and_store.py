#!/usr/bin/env python3
# process_and_store.py – NSE option-metrics ETL                © Sajal Tech 2025
# ─────────────────────────────────────────────────────────────────────────────
import os, sys, json
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

from db_util import DBWriter

# ── project-local imports ───────────────────────────────────────────────────
sys.path.append(str(Path(__file__).resolve().parents[1]))  # add project root
from process_data import (                       # noqa: E402
    black_scholes_greeks,
    implied_volatility_bisection,
    compute_yz_rolling_vol,
    get_rate_with_fallback,
    pick_strike_nearest_underlying,
    get_time_to_expiry_in_years,
)

WINDOW, MAX_LOOKBACK = 30, 90           # Y-Z parameters
# ─────────────────────────────────────────────────────────────────────────────
def _safe_T(date_key, expiry_str):
    return get_time_to_expiry_in_years(date_key, expiry_str) if expiry_str else 0.0


def _load_bhavcopy(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "SYMBOL" in df.columns:
        return df
    df = pd.read_csv(path, skiprows=2)
    if "SYMBOL" in df.columns:
        return df
    raise ValueError(f"'SYMBOL' column not found in {path.name}")


# ─────────────────────────────────────────────────────────────────────────────
def main():
    db_writer  = DBWriter()
    SCRIPT_DIR = Path(__file__).resolve().parent
    ROOT_DIR   = SCRIPT_DIR.parent

    bhavcopy_dir = ROOT_DIR / "bhavcopy" / "extracted"
    interest_csv = ROOT_DIR / "interest_rates" / "ADJUSTED_MIFOR.csv"
    yahoo_dir    = ROOT_DIR / "yahoo_finance"
    earnings_json = ROOT_DIR / "earning_dates" / "earning_dates.json"

    # ── symbol list ────────────────────────────────────────────────────────
    with open(ROOT_DIR / "nse_fno_scripts.json") as f:
        fno = json.load(f)
    symbols = [x["symbol"] for g in ("index_futures", "individual_securities") for x in fno[g]]

    # ── spot-price cache (Yahoo) ───────────────────────────────────────────
    spot_price_map = {}
    for sym in symbols:
        fp = yahoo_dir / f"{sym}_processed.json"
        if not fp.exists():
            continue
        ts = json.load(fp)["historical"]["scripts"][sym]["timestamps"]
        spot_price_map[sym] = {d: v["underlying_price"] for d, v in ts.items()}

    # ── interest-rate map ─────────────────────────────────────────────────
    ir_df = pd.read_csv(interest_csv, skiprows=2)
    ir_df["date"] = pd.to_datetime(ir_df["Date"], format="%d %b %Y", errors="coerce")
    ir_map = {d.date(): float(r["FBIL ADJUSTED MIFOR(%)"])
              for d, r in ir_df.groupby("date").first().iterrows()}

    # ── earnings-date map ─────────────────────────────────────────────────
    earnings_map = {}
    if earnings_json.exists():
        with open(earnings_json) as fh:
            for rec in json.load(fh):
                if rec.get("event_type") != "stock_results":
                    continue
                sym  = rec.get("trading_symbol")
                date = rec.get("date")
                try:
                    dt = datetime.strptime(date, "%Y-%m-%d")
                    earnings_map.setdefault(sym, []).append(dt)
                except Exception:
                    pass
        # keep sorted lists
        for sym in earnings_map:
            earnings_map[sym].sort()

    # ── OHLC holder for RV calc ───────────────────────────────────────────
    daily_ohlc = {s: [] for s in symbols}

    # ── iterate bhav-copy files ───────────────────────────────────────────
    for csv_file in sorted(bhavcopy_dir.glob("*.csv")):
        filename = csv_file.name
        date_obj = datetime.strptime(filename[2:-8], "%d%b%Y")
        date_key = date_obj.strftime("%d-%b-%Y")
        r_dec    = get_rate_with_fallback(date_obj.date(), ir_map) / 100.0

        df = _load_bhavcopy(csv_file)
        print(f"Processing {filename}")

        for sym in symbols:
            sym_df = df[df["SYMBOL"] == sym]
            if sym_df.empty:
                continue

            # ── FUT OHLC (nearest expiry) ───────────────────────────────
            fut = sym_df[sym_df["INSTRUMENT"].isin(["FUTSTK", "FUTIDX"])].copy()
            if fut.empty():
                continue
            fut["EXPIRY_DT"] = pd.to_datetime(fut["EXPIRY_DT"], errors="coerce").fillna(pd.Timestamp("2100-01-01"))
            fut_row = fut.sort_values("EXPIRY_DT").iloc[0]

            # only add bar if all OHLC > 0  (prevents NaN RV)
            if all(float(fut_row[x]) > 0 for x in ("OPEN", "HIGH", "LOW", "CLOSE")):
                daily_ohlc[sym].append(
                    dict(Date=date_obj,
                         Open=float(fut_row["OPEN"]),
                         High=float(fut_row["HIGH"]),
                         Low=float(fut_row["LOW"]),
                         Close=float(fut_row["CLOSE"]))
                )

            # ── inline Yang-Zhang RV ───────────────────────────────────
            rv_yz_today = None
            rows = daily_ohlc[sym]
            if len(rows) >= WINDOW:
                df_tmp = pd.DataFrame(rows[-MAX_LOOKBACK:])
                yz = compute_yz_rolling_vol(df_tmp, window=WINDOW, max_lookback=MAX_LOOKBACK, trading_periods=252)
                if not yz.empty and np.isfinite(yz.iloc[-1]):
                    rv_yz_today = float(yz.iloc[-1])

            # ── spot price (Yahoo → futures close fallback) ────────────
            spot = spot_price_map.get(sym, {}).get(date_key) or float(fut_row["CLOSE"])

            # ── upcoming earnings date ─────────────────────────────────
            upcoming_earn = None
            for dt in earnings_map.get(sym, []):
                if dt > date_obj:
                    upcoming_earn = dt.strftime("%d-%b-%Y")
                    break

            # ── base payload ──────────────────────────────────────────
            payload = {
                "underlying_price": spot,
                "interest_rate":    r_dec * 100,
                "expiry_30d": None,
                "expiry_60d": None,
                "expiry_90d": None,
                "rv_yz": rv_yz_today,
                "strike_price": None,
                "upcoming_earning_date": upcoming_earn,
            }

            # expiries
            exps = sorted(fut["EXPIRY_DT"].dropna().unique())
            if len(exps) > 0: payload["expiry_30d"] = exps[0].strftime("%d-%b-%Y")
            if len(exps) > 1: payload["expiry_60d"] = exps[1].strftime("%d-%b-%Y")
            if len(exps) > 2: payload["expiry_90d"] = exps[2].strftime("%d-%b-%Y")

            # ── option chain snapshot & IV calculations (unchanged) ────
            opts = sym_df[sym_df["INSTRUMENT"].isin(["OPTSTK", "OPTIDX"])].copy()
            opts["EXPIRY_DT"] = pd.to_datetime(opts["EXPIRY_DT"], errors="coerce")

            chain = []
            for _, row in opts.iterrows():
                try:
                    exp = row["EXPIRY_DT"].strftime("%d-%b-%Y")
                    T   = get_time_to_expiry_in_years(date_key, exp)
                    iv  = implied_volatility_bisection(
                            float(row["SETTLE_PR"]), spot, float(row["STRIKE_PR"]), T,
                            r_dec, is_call=row["OPTION_TYP"] == "CE")
                    delta = black_scholes_greeks(
                            spot, float(row["STRIKE_PR"]), T, r_dec, iv,
                            is_call=row["OPTION_TYP"] == "CE")["delta"]
                    chain.append(
                        dict(expiry=exp, strike=float(row["STRIKE_PR"]),
                             type=row["OPTION_TYP"], settle=float(row["SETTLE_PR"]),
                             iv=iv*100 if iv else 0, delta=delta,
                             volume=int(row["CONTRACTS"]))
                    )
                except Exception:
                    pass
            payload["option_chain"] = chain

            # ── IV30/60/90 (unchanged) ─────────────────────────────────
            nearest = pick_strike_nearest_underlying(spot, opts)
            if nearest:
                ce30, ce60, ce90, pe30, pe60, pe90, strike = nearest
                if any(x is None for x in (ce30, ce60, ce90, pe30, pe60, pe90)):
                    continue
                payload["strike_price"] = strike

                def _iv(prem, T, call=True):
                    return implied_volatility_bisection(prem, spot, strike, T, r_dec, is_call=call) * 100

                T30 = _safe_T(date_key, payload["expiry_30d"])
                T60 = _safe_T(date_key, payload["expiry_60d"])
                T90 = _safe_T(date_key, payload["expiry_90d"])

                payload["ce"] = {
                    "iv_30": _iv(float(ce30["SETTLE_PR"]), T30, True),
                    "iv_60": _iv(float(ce60["SETTLE_PR"]), T60, True),
                    "iv_90": _iv(float(ce90["SETTLE_PR"]), T90, True),
                    "volume": int(opts[opts["OPTION_TYP"] == "CE"]["CONTRACTS"].sum()),
                }
                payload["pe"] = {
                    "iv_30": _iv(float(pe30["SETTLE_PR"]), T30, False),
                    "iv_60": _iv(float(pe60["SETTLE_PR"]), T60, False),
                    "iv_90": _iv(float(pe90["SETTLE_PR"]), T90, False),
                    "volume": int(opts[opts["OPTION_TYP"] == "PE"]["CONTRACTS"].sum()),
                }

            payload["extras"] = {"source_file": filename}

            db_writer.write_row(sym, date_key, payload)

    db_writer.close()


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
