import streamlit as st
import pandas as pd
from datetime import date, timedelta
import io
import os
import subprocess

# Ensure Playwright browsers are installed (for Streamlit Cloud)
try:
    from playwright.async_api import async_playwright
except ImportError:
    os.system("pip install playwright")

# Check if we need to install browser binaries
if not os.path.exists("playwright_installed.flag"):
    st.info("Installing browser drivers for the first time... this may take a minute.")
    subprocess.run(["playwright", "install", "chromium"], check=True)
    # Create a flag file so we don't run this every time (optional, but good for restart speed)
    with open("playwright_installed.flag", "w") as f:
        f.write("installed")

from scraper import get_data

# Set page config
st.set_page_config(page_title="Israel Real Estate Scraper", page_icon="🏘️")

st.title("🏘️ Israel Tax Authority Data Scraper")
st.markdown("""
This tool allows you to scrape real estate transaction data from the **Israel Tax Authority** (Nadlan.gov.il).
Enter a city and a date range below to generate an Excel report.
""")

from cities import ISRAEL_CITIES
from neighborhoods import get_neighborhoods

# Sidebar inputs
st.sidebar.header("Search Parameters")
# city_input = st.sidebar.text_input("City Name (Hebrew)", value="באר שבע")
city_input = st.sidebar.selectbox("City Name (Hebrew)", options=ISRAEL_CITIES, index=ISRAEL_CITIES.index("באר שבע") if "באר שבע" in ISRAEL_CITIES else 0)

# Neighborhood selection (Dependent on City)
neighborhood_options = get_neighborhoods(city_input)
# Add "Type Manually" option
selection_mode = st.sidebar.radio("Neighborhood Selection:", ["Choose from List", "Type Manually"], horizontal=True)

if selection_mode == "Choose from List":
    neighborhood_input = st.sidebar.selectbox("Neighborhood", options=neighborhood_options)
else:
    neighborhood_input = st.sidebar.text_input("Enter Neighborhood Name")

start_date = st.sidebar.date_input("Start Date", value=date.today() - timedelta(days=30))
end_date = st.sidebar.date_input("End Date", value=date.today())

st.sidebar.header("Filters")
min_rooms, max_rooms = st.sidebar.slider(
    "Select Room Range",
    min_value=1.0, 
    max_value=10.0, 
    value=(1.0, 10.0),
    step=0.5
)

# Main section
if st.sidebar.button("Start Scraping", type="primary"):
    if not city_input:
        st.error("Please enter a city name.")
    else:
        status_text = f"Scraping data for {city_input}"
        if neighborhood_input and neighborhood_input != "כל השכונות":
            status_text += f" ({neighborhood_input})"
        status_text += f" | Rooms: {min_rooms}-{max_rooms}"
        status_text += "... this may take a minute."
        
        with st.spinner(status_text):
            try:
                # Call the scraper
                # Pass room parameters
                df = get_data(city_input, start_date, end_date, neighborhood_input, min_rooms=min_rooms, max_rooms=max_rooms)
                
                if not df.empty:
                    st.success(f"Successfully scraped {len(df)} transactions!")
                    
                    # Data Processing for Visualization
                    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
                    df = df.sort_values('Date')
                    
                    # Display Data (Collapsible)
                    with st.expander(f"View Data ({len(df)} rows)", expanded=False):
                        st.dataframe(df)

                    # Charts Section
                    st.subheader("Price Trends Analysis")
                    
                    # 1. Price vs Date (Scatter/Line)
                    # Resample to monthly average of Price PER SQM
                    df_trend = df.set_index('Date')
                    # Ensure numeric conversion if needed, though scraper returns ints
                    monthly_avg = df_trend['Price/Sqm'].resample('ME').mean()
                    
                    st.line_chart(monthly_avg)
                    st.caption("Average Price per Sqm over Time (Monthly Aggregation) - מחיר למ״ר")
                    
                    # Convert to Excel in memory
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    # ... rest of code
                        df.to_excel(writer, index=False, sheet_name='Transactions')
                        
                        # Format the Excel (optional polish)
                        workbook = writer.book
                        worksheet = writer.sheets['Transactions']
                        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#DCE6F1', 'border': 1})
                        for col_num, value in enumerate(df.columns.values):
                            worksheet.write(0, col_num, value, header_fmt)
                        worksheet.set_column('A:Z', 20)
                        
                    # Download button
                    st.download_button(
                        label="📥 Download Excel File",
                        data=buffer.getvalue(),
                        file_name=f"nadlan_data_{city_input}_{date.today()}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.warning("לא נמצאו נתונים עבור החיפוש המבוקש. אנא ודא ששם השכונה/העיר אוייתו כהלכה ונסה שנית.")
                    
            except Exception as e:
                st.error(f"Redaction Error: {e}")

st.markdown("---")
st.caption("Note: This tool uses Playwright for automation. Ensure you have the necessary drivers installed if running locally.")
