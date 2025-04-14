import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import shutil

# Setup base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.join(BASE_DIR, 'analysis')
PLOTS_DIR = os.path.join(ANALYSIS_DIR, 'plots')

# Plot styling
plt.style.use('ggplot')
sns.set(font_scale=1.2)
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['savefig.dpi'] = 100
plt.rcParams['figure.dpi'] = 100

# Load data
csv_path = os.path.join(ANALYSIS_DIR, 'processed_data.csv')
df = pd.read_csv(csv_path)
df['date'] = pd.to_datetime(df['date'])

# Ensure plots directory exists
if os.path.isfile(PLOTS_DIR):
    os.remove(PLOTS_DIR)
os.makedirs(PLOTS_DIR, exist_ok=True)

# 1. Time Series Plot of IV vs RV
plt.figure()
plt.plot(df['date'], df['avg_iv_30'], label='Avg IV 30d')
plt.plot(df['date'], df['avg_iv_60'], label='Avg IV 60d')
plt.plot(df['date'], df['avg_iv_90'], label='Avg IV 90d')
plt.plot(df['date'], df['rv_yz'], label='RV (YZ) %', linestyle='--')
plt.title('Time Series of Implied Volatility vs Realized Volatility')
plt.xlabel('Date')
plt.ylabel('Volatility (%)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'iv_vs_rv_timeseries.png'))
plt.close()

# 2. CE IV comparison
plt.figure()
plt.plot(df['date'], df['ce_iv_30'], label='CE IV 30d')
plt.plot(df['date'], df['ce_iv_60'], label='CE IV 60d')
plt.plot(df['date'], df['ce_iv_90'], label='CE IV 90d')
plt.title('Call Option Implied Volatility')
plt.xlabel('Date')
plt.ylabel('Implied Volatility (%)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'ce_iv_comparison.png'))
plt.close()

# 2b. PE IV comparison
plt.figure()
plt.plot(df['date'], df['pe_iv_30'], label='PE IV 30d')
plt.plot(df['date'], df['pe_iv_60'], label='PE IV 60d')
plt.plot(df['date'], df['pe_iv_90'], label='PE IV 90d')
plt.title('Put Option Implied Volatility')
plt.xlabel('Date')
plt.ylabel('Implied Volatility (%)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'pe_iv_comparison.png'))
plt.close()

# 3. IV Skew
plt.figure()
plt.plot(df['date'], df['ce_iv_30'] - df['pe_iv_30'], label='IV Skew 30d')
plt.plot(df['date'], df['ce_iv_60'] - df['pe_iv_60'], label='IV Skew 60d')
plt.plot(df['date'], df['ce_iv_90'] - df['pe_iv_90'], label='IV Skew 90d')
plt.axhline(0, color='black', linestyle='--', alpha=0.5)
plt.title('IV Skew (CE - PE)')
plt.xlabel('Date')
plt.ylabel('IV Skew (%)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'iv_skew.png')) 
plt.close()

# 4. IV vs Underlying Price
plt.figure()
plt.scatter(df['underlying_price'], df['avg_iv_30'], alpha=0.7)
plt.title('Implied Volatility vs Underlying Price')
plt.xlabel('Underlying Price')
plt.ylabel('Avg IV 30d (%)')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'iv_vs_price.png'))
plt.close()

# 5. IV Percentile Distribution
plt.figure()
plt.hist(df['ce_iv_30d_percentile'].dropna(), bins=20, alpha=0.6, label='CE IV 30d')
plt.hist(df['pe_iv_30d_percentile'].dropna(), bins=20, alpha=0.6, label='PE IV 30d')
plt.title('IV 30d Percentile Distribution')
plt.xlabel('Percentile')
plt.ylabel('Frequency')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'iv_percentile_distribution.png'))
plt.close()

# 6. IV Rank Distribution
plt.figure()
plt.hist(df['ce_iv_30d_rank'].dropna(), bins=20, alpha=0.6, label='CE IV Rank')
plt.hist(df['pe_iv_30d_rank'].dropna(), bins=20, alpha=0.6, label='PE IV Rank')
plt.title('IV 30d Rank Distribution')
plt.xlabel('Rank')
plt.ylabel('Frequency')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'iv_rank_distribution.png'))
plt.close()

# 7. IV Term Structure (CE)
dates_to_analyze = df['date'].iloc[::len(df)//5].tolist()
plt.figure()
for date in dates_to_analyze:
    row = df[df['date'] == date].iloc[0]
    plt.plot([30, 60, 90], [row['ce_iv_30'], row['ce_iv_60'], row['ce_iv_90']],
             marker='o', label=f'CE {date.strftime("%Y-%m-%d")}')
plt.title('Call Option IV Term Structure')
plt.xlabel('Days to Expiry')
plt.ylabel('Implied Volatility (%)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'ce_iv_term_structure.png'))
plt.close()

# 7b. IV Term Structure (PE)
plt.figure()
for date in dates_to_analyze:
    row = df[df['date'] == date].iloc[0]
    plt.plot([30, 60, 90], [row['pe_iv_30'], row['pe_iv_60'], row['pe_iv_90']],
             marker='o', label=f'PE {date.strftime("%Y-%m-%d")}')
plt.title('Put Option IV Term Structure')
plt.xlabel('Days to Expiry')
plt.ylabel('Implied Volatility (%)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'pe_iv_term_structure.png'))
plt.close()

# 8. IV vs RV Scatter
plt.figure()
plt.scatter(df['rv_yz'], df['avg_iv_30'], alpha=0.7)
plt.plot([0, 100], [0, 100], 'k--', alpha=0.5)
plt.title('IV (30d) vs Realized Volatility (YZ)')
plt.xlabel('Realized Volatility (%)')
plt.ylabel('Implied Volatility (%)')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'iv_vs_rv_scatter.png'))
plt.close()

# 9. IV-RV Spread
df['iv_rv_spread'] = df['avg_iv_30'] - (df['rv_yz'])
plt.figure()
plt.plot(df['date'], df['iv_rv_spread'])
plt.axhline(0, color='black', linestyle='--', alpha=0.5)
plt.title('IV-RV Spread (30d) Over Time')
plt.xlabel('Date')
plt.ylabel('IV - RV (%)')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, 'iv_rv_spread.png'))
plt.close()

print(f"All plots have been generated and saved to: {PLOTS_DIR}")
