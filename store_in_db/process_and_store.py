#!/usr/bin/env python3
# process_and_store.py – NSE option-metrics ETL                © Sajal Tech 2025
# ─────────────────────────────────────────────────────────────────────────────
import json, sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from db_util import DBWriter

# ═════════════════ project-local imports ═══════════════════════════════════
sys.path.append(str(Path(__file__).resolve().parents[1]))  # add project root

from process_data import (                     # noqa: E402
    black_scholes_greeks,
    implied_volatility_bisection,
    compute_yz_rolling_vol,
    get_rate_with_fallback,
    pick_strike_nearest_underlying,
    get_time_to_expiry_in_years,
)

WINDOW, MAX_LOOKBACK = 30, 90   # Yang–Zhang parameters
# ────────────────────────────────────────────────────────────────────────────
def _safe_T(date_key: str, expiry: str | None) -> float:
    return get_time_to_expiry_in_years(date_key, expiry) if expiry else 0.0


def _load_bhavcopy(path: Path) -> pd.DataFrame:
    """Read an NSE bhav-copy CSV; retry with skiprows=2 if first lines are metadata."""
    df = pd.read_csv(path)
    if "SYMBOL" in df.columns:
        return df
    df = pd.read_csv(path, skiprows=2)
    if "SYMBOL" in df.columns:
        return df
    raise ValueError(f"'SYMBOL' column missing in {path.name}")


# ────────────────────────────────────────────────────────────────────────────
def main() -> None:
    ROOT = Path(__file__).resolve().parents[1]

    bhavcopy_dir  = ROOT / "bhavcopy" / "extracted"
    interest_csv  = ROOT / "interest_rates" / "ADJUSTED_MIFOR.csv"
    yahoo_dir     = ROOT / "yahoo_finance"
    earnings_json = ROOT / "earning_dates" / "earning_dates.json"

    # ── symbol list ───────────────────────────────────────────────────────
    with open(ROOT / "nse_fno_scripts.json") as fh:
        fno = json.load(fh)
    symbols = [x["symbol"]
               for g in ("index_futures", "individual_securities")
               for x in fno[g]]

    # ── spot-price cache (Yahoo) ──────────────────────────────────────────
    spot_price_map: dict[str, dict[str, float]] = {}
    for sym in symbols:
        fp = yahoo_dir / f"{sym}_processed.json"
        if not fp.exists():
            continue
        with fp.open(encoding="utf-8") as fh:
            ts = json.load(fh)["historical"]["scripts"][sym]["timestamps"]
        spot_price_map[sym] = {d: v["underlying_price"] for d, v in ts.items()}

    # ── daily interest-rate map ───────────────────────────────────────────
    ir_df = pd.read_csv(interest_csv, skiprows=2)
    ir_df["dt"] = pd.to_datetime(ir_df["Date"], format="%d %b %Y", errors="coerce")
    ir_map = {d.date(): float(r["FBIL ADJUSTED MIFOR(%)"])
              for d, r in ir_df.groupby("dt").first().iterrows()}

    # ── earnings map ──────────────────────────────────────────────────────
    earnings_map: dict[str, list[datetime]] = {}
    if earnings_json.exists():
        with open(earnings_json) as fh:
            for rec in json.load(fh):
                if rec.get("event_type") != "stock_results":
                    continue
                sym, date_s = rec.get("trading_symbol"), rec.get("date")
                try:
                    earnings_map.setdefault(sym, []).append(
                        datetime.strptime(date_s, "%Y-%m-%d"))
                except Exception:
                    pass
        for sym in earnings_map:
            earnings_map[sym].sort()

    # ── holder for realised-vol bars ──────────────────────────────────────
    daily_ohlc: dict[str, list[dict]] = {s: [] for s in symbols}

    db_writer = DBWriter()

    # ── iterate bhav-copies ───────────────────────────────────────────────
    for csv in sorted(bhavcopy_dir.glob("*.csv")):
        fname    = csv.name
        date_obj = datetime.strptime(fname[2:-8], "%d%b%Y")
        date_key = date_obj.strftime("%d-%b-%Y")
        r_dec    = get_rate_with_fallback(date_obj.date(), ir_map) / 100.0
        df       = _load_bhavcopy(csv)

        print("Processing", fname)

        for sym in symbols:
            sym_df = df[df["SYMBOL"] == sym]
            if sym_df.empty:
                continue

            # ── nearest-expiry futures bar ──────────────────────────────
            fut = sym_df[sym_df["INSTRUMENT"].isin(["FUTSTK", "FUTIDX"])].copy()
            if fut.empty:
                continue
            fut["EXPIRY_DT"] = pd.to_datetime(
                fut["EXPIRY_DT"], errors="coerce").fillna(pd.Timestamp("2100-01-01"))
            fut_row = fut.sort_values("EXPIRY_DT").iloc[0]

            if all(float(fut_row[x]) > 0 for x in ("OPEN", "HIGH", "LOW", "CLOSE")):
                daily_ohlc[sym].append(dict(
                    Date=date_obj,
                    Open=float(fut_row["OPEN"]),
                    High=float(fut_row["HIGH"]),
                    Low=float(fut_row["LOW"]),
                    Close=float(fut_row["CLOSE"]),
                ))

            rv_yz = None
            if len(daily_ohlc[sym]) >= WINDOW:
                yz = compute_yz_rolling_vol(pd.DataFrame(
                          daily_ohlc[sym][-MAX_LOOKBACK:]),
                          window=WINDOW, max_lookback=MAX_LOOKBACK, trading_periods=252)
                if not yz.empty and np.isfinite(yz.iloc[-1]):
                    rv_yz = float(yz.iloc[-1])

            # ── spot price ──────────────────────────────────────────────
            spot = spot_price_map.get(sym, {}).get(date_key) \
                   or float(fut_row["CLOSE"])

            # ── upcoming earnings ───────────────────────────────────────
            upcoming = next((dt.strftime("%d-%b-%Y")
                             for dt in earnings_map.get(sym, [])
                             if dt > date_obj), None)

            # ── payload skeleton ────────────────────────────────────────
            payload = {
                "underlying_price": spot,
                "interest_rate": r_dec * 100,
                "expiry_30d": None, "expiry_60d": None, "expiry_90d": None,
                "rv_yz": rv_yz,
                "strike_price": None,
                "upcoming_earning_date": upcoming,
            }

            # futures expiries
            exps = sorted(fut["EXPIRY_DT"].dropna().unique())
            if len(exps) > 0: payload["expiry_30d"] = exps[0].strftime("%d-%b-%Y")
            if len(exps) > 1: payload["expiry_60d"] = exps[1].strftime("%d-%b-%Y")
            if len(exps) > 2: payload["expiry_90d"] = exps[2].strftime("%d-%b-%Y")

            # ── option chain snapshot ───────────────────────────────────
            opts = sym_df[sym_df["INSTRUMENT"].isin(["OPTSTK", "OPTIDX"])].copy()
            opts["EXPIRY_DT"] = pd.to_datetime(opts["EXPIRY_DT"], errors="coerce")

            chain = []
            for _, row in opts.iterrows():
                try:
                    exp = row["EXPIRY_DT"].strftime("%d-%b-%Y")
                    T   = get_time_to_expiry_in_years(date_key, exp)
                    iv  = implied_volatility_bisection(
                            float(row["SETTLE_PR"]), spot, float(row["STRIKE_PR"]),
                            T, r_dec, is_call=row["OPTION_TYP"] == "CE")
                    delta = black_scholes_greeks(
                            spot, float(row["STRIKE_PR"]), T, r_dec, iv,
                            is_call=row["OPTION_TYP"] == "CE")["delta"]
                    chain.append(dict(
                        expiry=exp, strike=float(row["STRIKE_PR"]),
                        type=row["OPTION_TYP"], settle=float(row["SETTLE_PR"]),
                        iv=iv*100 if iv else 0, delta=delta,
                        volume=int(row["CONTRACTS"]),
                    ))
                except Exception:
                    pass
            payload["option_chain"] = chain

            # ── fill CE / PE detail ────────────────────────────────────
            nearest = pick_strike_nearest_underlying(spot, opts)
            if not nearest:
                continue
            ce30, ce60, ce90, pe30, pe60, pe90, strike = nearest
            if any(x is None for x in (ce30, ce60, ce90, pe30, pe60, pe90)):
                continue
            payload["strike_price"] = strike

            def _iv(p, T, call=True):
                return implied_volatility_bisection(
                    p, spot, strike, T, r_dec, is_call=call) * 100

            T30 = _safe_T(date_key, payload["expiry_30d"])
            T60 = _safe_T(date_key, payload["expiry_60d"])
            T90 = _safe_T(date_key, payload["expiry_90d"])

            # helper: greeks or zeros
            def _greeks(iv30: float, call=True):
                if iv30 > 0 and T30 > 0:
                    return black_scholes_greeks(
                        spot, strike, T30, r_dec, iv30 / 100.0, is_call=call)
                return {"delta": 0.0, "gamma": 0.0,
                        "theta": 0.0, "vega": 0.0, "rho": 0.0}

            # CE block
            iv30_ce = _iv(float(ce30["SETTLE_PR"]), T30, True)
            payload["ce"] = {
                "iv_30": iv30_ce,
                "iv_60": _iv(float(ce60["SETTLE_PR"]), T60, True),
                "iv_90": _iv(float(ce90["SETTLE_PR"]), T90, True),
                "volume": int(opts[opts["OPTION_TYP"]=="CE"]["CONTRACTS"].sum()),
                "last_price_30d": float(ce30["SETTLE_PR"]),
                "close": float(ce30["CLOSE"]),
                "open":  float(ce30["OPEN"]),
                "high":  float(ce30["HIGH"]),
                "low":   float(ce30["LOW"]),
                "ivp": None, "ivr": None,
                "greeks": _greeks(iv30_ce, True),
            }

            # PE block
            iv30_pe = _iv(float(pe30["SETTLE_PR"]), T30, False)
            payload["pe"] = {
                "iv_30": iv30_pe,
                "iv_60": _iv(float(pe60["SETTLE_PR"]), T60, False),
                "iv_90": _iv(float(pe90["SETTLE_PR"]), T90, False),
                "volume": int(opts[opts["OPTION_TYP"]=="PE"]["CONTRACTS"].sum()),
                "last_price_30d": float(pe30["SETTLE_PR"]),
                "close": float(pe30["CLOSE"]),
                "open":  float(pe30["OPEN"]),
                "high":  float(pe30["HIGH"]),
                "low":   float(pe30["LOW"]),
                "ivp": None, "ivr": None,
                "greeks": _greeks(iv30_pe, False),
            }

            payload["extras"] = {"source_file": fname}
            db_writer.write_row(sym, date_key, payload)

    db_writer.close()


# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
