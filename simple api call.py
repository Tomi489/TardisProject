import requests
import pandas as pd
import io
import gzip
import os

# The folder where we will save files
userpc=os.getenv("USERPROFILE")
output_folder = os.path.join(userpc, "Downloads", "tardis_sample_data")

# The specific Option Contract and Date
symbol = "BTC-250502-99000-C"
year, month, day = "2025", "05", "01"

# The list of all possible data types (from your error message)
data_types = [
    'quotes',                
    'derivative_ticker',     
    'trades',
    'book_ticker',
    'liquidations',
    'options_chain',
    'book_snapshot_5',
    'book_snapshot_25',
    'incremental_book_L2'
]

# Create the directory if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    print(f"Created folder: {output_folder}")

print(f"Processing data for {symbol} on {year}-{month}-{day}...\n")

for dtype in data_types:
    # Construct the URL
    url = f"https://datasets.tardis.dev/v1/binance-european-options/{dtype}/{year}/{month}/{day}/{symbol}.csv.gz"
    
    print(f"Checking {dtype}...", end=" ")
    
    try:
        # Stream the request so we don't download huge files into memory immediately
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            print("Found! Processing...", end=" ")
            
            # Decompress the stream on the fly
            with gzip.open(io.BytesIO(response.content), 'rt') as f:
                
                # LOGIC: Full download for 'quotes', Sample for others
                if dtype == 'quotes':
                    # Read the WHOLE file (might take a moment)
                    df = pd.read_csv(f)
                    filename = f"FULL_{dtype}.csv"
                    print(f"Saving {len(df)} rows...", end=" ")
                else:
                    # Read only first 100 rows
                    df = pd.read_csv(f, nrows=100)
                    filename = f"SAMPLE_{dtype}.csv"
                    print(f"Sampling 100 rows...", end=" ")

                # Save to the target folder
                save_path = os.path.join(output_folder, filename)
                df.to_csv(save_path, index=False)
                print(f"Saved to {filename}")

        elif response.status_code == 404:
            print("Not found (No data for this specific option).")
        else:
            print(f"Error: Status {response.status_code}")

    except Exception as e:
        print(f"Error: {e}")

print(f"\nDone! Check your folder: {output_folder}")