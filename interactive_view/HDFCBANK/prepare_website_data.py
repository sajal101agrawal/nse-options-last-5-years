import pandas as pd
import json

# Function to convert CSV to JSON for the website
def convert_csv_to_json():
    try:
        # Read the processed data CSV
        df = pd.read_csv('/home/ubuntu/analysis/processed_data.csv')
        
        # Convert date to string format for JSON
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        # Create JSON structure
        json_data = {
            'dates': df['date'].tolist(),
            'ceIV30': df['ce_iv_30'].tolist(),
            'ceIV60': df['ce_iv_60'].tolist(),
            'ceIV90': df['ce_iv_90'].tolist(),
            'peIV30': df['pe_iv_30'].tolist(),
            'peIV60': df['pe_iv_60'].tolist(),
            'peIV90': df['pe_iv_90'].tolist(),
            'rvYZ': (df['rv_yz'] * 100).tolist(),  # Convert to percentage
            'underlyingPrice': df['underlying_price'].tolist(),
            'strikePrice': df['strike_price'].tolist()
        }
        
        # Calculate average IVs
        df['avgIV30'] = (df['ce_iv_30'] + df['pe_iv_30']) / 2
        df['avgIV60'] = (df['ce_iv_60'] + df['pe_iv_60']) / 2
        df['avgIV90'] = (df['ce_iv_90'] + df['pe_iv_90']) / 2
        
        json_data['avgIV30'] = df['avgIV30'].tolist()
        json_data['avgIV60'] = df['avgIV60'].tolist()
        json_data['avgIV90'] = df['avgIV90'].tolist()
        
        # Calculate IV-RV spread
        df['ivRvSpread'] = df['avgIV30'] - (df['rv_yz'] * 100)
        json_data['ivRvSpread'] = df['ivRvSpread'].tolist()
        
        # Write to JSON file
        with open('/home/ubuntu/website/data.json', 'w') as f:
            json.dump(json_data, f)
        
        print("Successfully converted CSV to JSON")
        return True
    except Exception as e:
        print(f"Error converting CSV to JSON: {e}")
        return False

# Function to copy the CSV file to the website directory
def copy_csv_to_website():
    try:
        # Read the CSV file
        df = pd.read_csv('/home/ubuntu/analysis/processed_data.csv')
        
        # Write to the website directory
        df.to_csv('/home/ubuntu/website/processed_data.csv', index=False)
        
        print("Successfully copied CSV to website directory")
        return True
    except Exception as e:
        print(f"Error copying CSV to website directory: {e}")
        return False

# Main function
if __name__ == "__main__":
    convert_csv_to_json()
    copy_csv_to_website()
