from scraper import get_data
from datetime import date, timedelta
import pandas as pd
import os

def test_scraper():
    print("--- Starting Verify Test ---")
    city = "באר שבע"
    start = date.today() - timedelta(days=30)
    end = date.today()
    
    print(f"Testing scraper for {city} ({start} to {end})...")
    
    # Run the scraper logic
    try:
        df = get_data(city, start, end)
        
        if not df.empty:
            print(f"✅ Success! Scraped {len(df)} records.")
            print("Sample data:")
            print(df.head(3))
            
            # Test Excel generation
            output_file = "test_output.xlsx"
            df.to_excel(output_file)
            if os.path.exists(output_file):
                print(f"✅ Excel file created successfully: {output_file}")
            else:
                print("❌ Failed to create Excel file.")
        else:
            print("⚠️ Scraper ran but returned no data.")
            
    except Exception as e:
        print(f"❌ Error during execution: {e}")

if __name__ == "__main__":
    test_scraper()
