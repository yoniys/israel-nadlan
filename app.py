import streamlit as st
import pandas as pd
from datetime import date, timedelta
from scraper import get_data
import io

# Set page config
st.set_page_config(page_title="Israel Real Estate Scraper", page_icon="🏘️")

st.title("🏘️ Israel Tax Authority Data Scraper")
st.markdown("""
This tool allows you to scrape real estate transaction data from the **Israel Tax Authority** (Nadlan.gov.il).
Enter a city and a date range below to generate an Excel report.
""")

# Sidebar inputs
st.sidebar.header("Search Parameters")
city_input = st.sidebar.text_input("City Name (Hebrew)", value="באר שבע")
start_date = st.sidebar.date_input("Start Date", value=date.today() - timedelta(days=30))
end_date = st.sidebar.date_input("End Date", value=date.today())

# Main section
if st.sidebar.button("Start Scraping", type="primary"):
    if not city_input:
        st.error("Please enter a city name.")
    else:
        with st.spinner(f"Scraping data for {city_input}... this may take a minute."):
            try:
                # Call the scraper
                # Note: start_date and end_date from Streamlit are datetime.date objects
                df = get_data(city_input, start_date, end_date)
                
                if not df.empty:
                    st.success(f"Successfully scraped {len(df)} transactions!")
                    st.dataframe(df)
                    
                    # Convert to Excel in memory
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
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
                    st.warning("No data found for the specified criteria (or scraping failed).")
                    
            except Exception as e:
                st.error(f"An error occurred: {e}")

st.markdown("---")
st.caption("Note: This tool uses Playwright for automation. Ensure you have the necessary drivers installed if running locally.")
