import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import math
import os
import shutil

# Load the processed data
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print(BASE_DIR)

with open(os.path.join(BASE_DIR, 'processed_data.json'), 'r') as f:
    data = json.load(f)

# Extract HDFCBANK data
hdfcbank_data = data['historical']['scripts']['HDFCBANK']

# Extract data
extracted_data = []
for date, values in hdfcbank_data['timestamps'].items():
    if values.get('underlying_price') is not None and values.get('strike_price') is not None:
        entry = {
            'date': date,
            'underlying_price': values.get('underlying_price'),
            'strike_price': values.get('strike_price'),
            'rv_yz': values.get('rv_yz'),
            'expiry_30d': values.get('expiry_30d'),
            'expiry_60d': values.get('expiry_60d'),
            'expiry_90d': values.get('expiry_90d')
        }
        if 'ce' in values:
            entry['ce_iv_30'] = values['ce'].get('iv_30')
            entry['ce_iv_60'] = values['ce'].get('iv_60')
            entry['ce_iv_90'] = values['ce'].get('iv_90')
            entry['ce_iv_30d_percentile'] = values['ce'].get('ivp')
            entry['ce_iv_30d_rank'] = values['ce'].get('ivr')
        if 'pe' in values:
            entry['pe_iv_30'] = values['pe'].get('iv_30')
            entry['pe_iv_60'] = values['pe'].get('iv_60')
            entry['pe_iv_90'] = values['pe'].get('iv_90')
            entry['pe_iv_30d_percentile'] = values['pe'].get('ivp')
            entry['pe_iv_30d_rank'] = values['pe'].get('ivr')
        extracted_data.append(entry)

df = pd.DataFrame(extracted_data)
df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y')
df = df.sort_values('date')

# Average IVs
df['avg_iv_30'] = (df['ce_iv_30'] + df['pe_iv_30']) / 2
df['avg_iv_60'] = (df['ce_iv_60'] + df['pe_iv_60']) / 2
df['avg_iv_90'] = (df['ce_iv_90'] + df['pe_iv_90']) / 2

# IV verification
def verify_iv_calculations(df):
    results = {
        'iv_range_check': True,
        'iv_term_structure_check': True,
        'iv_variation_check': True,
        'anomalies': []
    }
    for col in ['ce_iv_30', 'ce_iv_60', 'ce_iv_90', 'pe_iv_30', 'pe_iv_60', 'pe_iv_90']:
        invalid_ivs = df[df[col] > 200][col].count() + df[df[col] < 0][col].count()
        if invalid_ivs > 0:
            results['iv_range_check'] = False
            results['anomalies'].append(f"Found {invalid_ivs} invalid values in {col}")
    term_structure_violations_ce = df[df['ce_iv_30'] > df['ce_iv_60']].shape[0]
    term_structure_violations_pe = df[df['pe_iv_30'] > df['pe_iv_60']].shape[0]
    if term_structure_violations_ce > 0.3 * df.shape[0]:
        results['iv_term_structure_check'] = False
        results['anomalies'].append(f"CE IV term structure violations: {term_structure_violations_ce}/{df.shape[0]}")
    if term_structure_violations_pe > 0.3 * df.shape[0]:
        results['iv_term_structure_check'] = False
        results['anomalies'].append(f"PE IV term structure violations: {term_structure_violations_pe}/{df.shape[0]}")
    for col in ['ce_iv_30', 'ce_iv_60', 'ce_iv_90', 'pe_iv_30', 'pe_iv_60', 'pe_iv_90']:
        if df[col].nunique() < 3:
            results['iv_variation_check'] = False
            results['anomalies'].append(f"{col} has only {df[col].nunique()} unique values")
    return results

# RV verification
def verify_rv_calculations(df):
    results = {
        'rv_range_check': True,
        'rv_variation_check': True,
        'rv_presence_check': True,
        'anomalies': []
    }
    df_rv = df[df['rv_yz'].notna()]
    invalid_rvs = df_rv[df_rv['rv_yz'] > 2].shape[0] + df_rv[df_rv['rv_yz'] < 0].shape[0]
    if invalid_rvs > 0:
        results['rv_range_check'] = False
        results['anomalies'].append(f"Found {invalid_rvs} invalid RV values")
    if df_rv['rv_yz'].nunique() < 2:
        results['rv_variation_check'] = False
        results['anomalies'].append(f"RV has only {df_rv['rv_yz'].nunique()} unique values")
    rv_presence_ratio = df_rv.shape[0] / df.shape[0]
    if rv_presence_ratio < 0.1:
        results['rv_presence_check'] = False
        results['anomalies'].append(f"RV is present in only {rv_presence_ratio:.1%} of dates")
    return results

# Run verifications
iv_verification = verify_iv_calculations(df)
rv_verification = verify_rv_calculations(df)

# Print results
print("IV Verification Results:")
print(f"IV Range Check: {'PASS' if iv_verification['iv_range_check'] else 'FAIL'}")
print(f"IV Term Structure Check: {'PASS' if iv_verification['iv_term_structure_check'] else 'FAIL'}")
print(f"IV Variation Check: {'PASS' if iv_verification['iv_variation_check'] else 'FAIL'}")
if iv_verification['anomalies']:
    print("IV Anomalies:")
    for a in iv_verification['anomalies']:
        print(f"  - {a}")

print("\nRV Verification Results:")
print(f"RV Range Check: {'PASS' if rv_verification['rv_range_check'] else 'FAIL'}")
print(f"RV Variation Check: {'PASS' if rv_verification['rv_variation_check'] else 'FAIL'}")
print(f"RV Presence Check: {'PASS' if rv_verification['rv_presence_check'] else 'FAIL'}")
if rv_verification['anomalies']:
    print("RV Anomalies:")
    for a in rv_verification['anomalies']:
        print(f"  - {a}")

# Save verification results
analysis_dir = os.path.join(BASE_DIR, 'analysis')
results_file = os.path.join(analysis_dir, 'verification_results.txt')

# If a directory exists with the same name, remove it
if os.path.isdir(results_file):
    shutil.rmtree(results_file)

os.makedirs(analysis_dir, exist_ok=True)

with open(results_file, 'w') as f:
    f.write("IV Verification Results:\n")
    f.write(f"IV Range Check: {'PASS' if iv_verification['iv_range_check'] else 'FAIL'}\n")
    f.write(f"IV Term Structure Check: {'PASS' if iv_verification['iv_term_structure_check'] else 'FAIL'}\n")
    f.write(f"IV Variation Check: {'PASS' if iv_verification['iv_variation_check'] else 'FAIL'}\n")
    if iv_verification['anomalies']:
        f.write("IV Anomalies:\n")
        for a in iv_verification['anomalies']:
            f.write(f"  - {a}\n")
    
    f.write("\nRV Verification Results:\n")
    f.write(f"RV Range Check: {'PASS' if rv_verification['rv_range_check'] else 'FAIL'}\n")
    f.write(f"RV Variation Check: {'PASS' if rv_verification['rv_variation_check'] else 'FAIL'}\n")
    f.write(f"RV Presence Check: {'PASS' if rv_verification['rv_presence_check'] else 'FAIL'}\n")
    if rv_verification['anomalies']:
        f.write("RV Anomalies:\n")
        for a in rv_verification['anomalies']:
            f.write(f"  - {a}\n")

# Save processed DataFrame
df.to_csv(os.path.join(BASE_DIR, 'analysis', 'processed_data.csv'), index=False)

print("\nVerification complete. Results saved to ./analysis/verification_results.txt")
print("Processed data saved to ./processed_data.csv")
