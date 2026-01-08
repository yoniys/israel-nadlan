import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
import pandas as pd
import datetime
from datetime import timedelta
from neighborhoods import get_neighborhoods
import numpy as np
import random # Still needed for fallback or partial mock augmentation if needed

nest_asyncio.apply()

async def fetch_nadlan_data(
    city: str, 
    start_date: datetime.date, 
    end_date: datetime.date, 
    neighborhood: str = None, 
    min_rooms: float = 0, 
    max_rooms: float = 100,
    min_floor: int = -10,
    max_floor: int = 200,
    min_sqm: int = 0,
    max_sqm: int = 10000,
    exclude_abnormal: bool = False,
    use_mock_data: bool = True
):
    
    # Format dates as DD/MM/YYYY
    start_str = start_date.strftime("%d/%m/%Y")
    end_str = end_date.strftime("%d/%m/%Y")
    
    print(f"[Scraper] Search: {city} ({neighborhood if neighborhood else 'All'}) {start_str}-{end_str}")
    print(f"[Scraper] Mode: {'MOCK' if use_mock_data else 'LIVE'} | Filters: Rooms {min_rooms}-{max_rooms}, Floor {min_floor}-{max_floor}, Sqm {min_sqm}-{max_sqm}, Exclude Abnormal: {exclude_abnormal}")
    
    data = []

    if use_mock_data:
        return await generate_mock_data(city, start_date, end_date, neighborhood, min_rooms, max_rooms, min_floor, max_floor, min_sqm, max_sqm, exclude_abnormal)

    # --- LIVE SCRAPING LOGIC ---
    try:
        async with async_playwright() as p:
            # Launch with headless=False so user can see what's happening if running locally
            # In cloud, this might fail if no UI, but usually headless=True is safer. 
            # We'll stick to headless=True for compatibility, but user can change it.
            browser = await p.chromium.launch(headless=True)
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                locale='he-IL',
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()

            # 1. Navigate
            url = "https://www.nadlan.gov.il/" 
            print(f"[Live Scraper] Navigating to {url}...")
            await page.goto(url, timeout=60000)
            
            # 2. Search
            # Selector identified: input#myInput2
            print(f"[Live Scraper] Typing city: {city}")
            try:
                await page.wait_for_selector('input#myInput2', state='visible', timeout=10000)
                await page.fill('input#myInput2', city)
                
                # Wait for autocomplete
                print("[Live Scraper] Waiting for autocomplete...")
                # Suggestion selector often: .react-autosuggest__suggestion or similar
                # We'll verify what the subagent saw or just wait a bit and hit enter (less reliable)
                # Subagent clicked: document.getElementById('react-autowhatever-1--item-0')
                await page.wait_for_selector('[id^="react-autowhatever-1--item-"]', timeout=5000)
                await page.click('[id^="react-autowhatever-1--item-0"]') # Click first suggestion
                
                # Click search
                # Selector identified: button.arrowBtn
                print("[Live Scraper] Clicking search...")
                await page.click('button.arrowBtn')
                
                # 3. Wait for Results
                print("[Live Scraper] Waiting for results table...")
                await page.wait_for_selector('tbody tr', timeout=15000)
                
                # 4. Parse Results
                rows = await page.query_selector_all('tbody tr')
                print(f"[Live Scraper] Found {len(rows)} rows. Parsing...")
                
                for row in rows:
                    cells = await row.query_selector_all('td')
                    if not cells: continue
                    
                    # Extract text
                    txts = [await c.inner_text() for c in cells]
                    # Structure is likely: 
                    # 0: Date, 1: Address, 2: Gush/Helka, 3: Asset Type, 4: Rooms, 5: Floor, 6: Sqm, 7: Price
                    # We need to be robust. Let's look for date pattern and price pattern.
                    
                    # Simple heuristic parsing
                    try:
                        # Find date
                        date_val = next((t for t in txts if '/' in t and len(t) == 10), None)
                        # Find price (numbers with comma)
                        price_val = next((t for t in txts if ',' in t and len(t) > 6), "0").replace(',', '')
                        
                        # Rooms (small float)
                        # Floor (small int)
                        # Sqm (medium int)
                        
                        # Fallback for now: map by expected index if standard
                        # Assuming: Date(0), Address(1), Type(2), Rooms(3), Floor(4), Sqm(5), Price(6)
                        if len(txts) >= 7:
                            row_date = txts[0]
                            row_addr = txts[1] # Contains neighborhood often
                            row_rooms = float(txts[3]) if txts[3].replace('.', '', 1).isdigit() else 0
                            row_floor = int(txts[4]) if txts[4].lstrip('-').isdigit() else 0
                            row_sqm = int(txts[5]) if txts[5].isdigit() else 0
                            row_price = int(txts[6].replace(',', '')) if txts[6].replace(',', '').isdigit() else 0
                            
                            # Calculate Price/Sqm
                            row_ppsqm = int(row_price / row_sqm) if row_sqm > 0 else 0
                            
                            item = {
                                "Date": row_date,
                                "City": city,
                                "Neighborhood": row_addr, # Approximation
                                "Asset Type": "דירה", # Scraped type
                                "Rooms": row_rooms,
                                "Floor": row_floor,
                                "Sqm": row_sqm,
                                "Price": row_price,
                                "Price/Sqm": row_ppsqm,
                                "Share": "100%", # Unknown from simple table usually
                                "Platform": "Nadlan.gov.il"
                            }
                            data.append(item)
                    except Exception as e:
                        print(f"Error parsing row: {e}")
                        continue

            except Exception as e:
                print(f"[Live Scraper Warning] UI Interaction failed: {e}")
                print("Returning empty dataframe or falling back.")
            
            await browser.close()

    except Exception as e:
        print(f"[Live Scraper Error] Critical failure: {e}")
    
    # If live scraping scraped nothing (due to block or error), warn user
    if not data and not use_mock_data:
        print("[Live Scraper] No data found. You might be blocked.")
        
    # Apply filters to the scraped data (or mock data returned earlier)
    # Note: filters are applied inside generate_mock_data, but for live data we need to apply them here
    df = pd.DataFrame(data)
    if not df.empty and not use_mock_data:
        # Filter Logic for Live Data
        if min_rooms > 0 or max_rooms < 100:
            df = df[(df['Rooms'] >= min_rooms) & (df['Rooms'] <= max_rooms)]
        if min_floor > -10 or max_floor < 200:
             df = df[(df['Floor'] >= min_floor) & (df['Floor'] <= max_floor)]
        if min_sqm > 0 or max_sqm < 10000:
             df = df[(df['Sqm'] >= min_sqm) & (df['Sqm'] <= max_sqm)]
        # Exclude abnormal for live data (simple outlier check)
        if exclude_abnormal:
             mean = df['Price/Sqm'].mean()
             std = df['Price/Sqm'].std()
             df = df[(df['Price/Sqm'] >= mean - 2*std) & (df['Price/Sqm'] <= mean + 2*std)]
            
    return df

async def generate_mock_data(city, start_date, end_date, neighborhood, min_rooms, max_rooms, min_floor, max_floor, min_sqm, max_sqm, exclude_abnormal):
    print("[Scraper] Generating MOCK DATA...")
    data = []
    delta = end_date - start_date
    num_records = random.randint(100, 300) # Smaller batch for speed
    
    platforms = ["Yad2", "Madlan", "Facebook Marketplace", "Komo", "Broker", "Direct from Developer"]
            
    for _ in range(num_records):
        random_days = random.randrange(delta.days + 1)
        d = start_date + timedelta(days=random_days)
        
        # Rooms
        weighted_rooms = [2, 2.5, 3, 3, 3, 3.5, 3.5, 4, 4, 4, 4.5, 5, 5, 6]
        if random.random() < 0.1: 
            rooms = random.choice([1, 1.5, 7, 8, 9, 10])
        else:
            rooms = random.choice(weighted_rooms)
        
        if not (min_rooms <= rooms <= max_rooms): continue

        # Floor
        floor = random.randint(-1, 25)
        if not (min_floor <= floor <= max_floor): continue
            
        # Sqm
        sqm = 40 + (rooms * 20) + random.randint(-10, 30)
        if not (min_sqm <= sqm <= max_sqm): continue
        
        # Share
        share = 1.0
        if random.random() < 0.05: share = random.choice([0.5, 0.33, 0.25, 0.1])
        if exclude_abnormal and share < 1.0: continue

        # Price 
        base_price = 1200000 + (rooms * 200000)
        price = (base_price + random.randint(-150000, 150000)) * share 
        
        if random.random() < 0.01: price = price * random.choice([0.5, 2.0]) # Outlier

        price_per_sqm = int(price / sqm)

        if neighborhood and neighborhood != "כל השכונות":
            curr_neighborhood = neighborhood
        else:
            curr_neighborhood = f"שכונה {random.choice(['א', 'ב', 'ג', 'ד', 'ה', 'נווה זאב', 'רמות', 'הנחלים', 'הפארק'])}"

        data.append({
            "Date": d.strftime("%d/%m/%Y"),
            "City": city,
            "Neighborhood": curr_neighborhood,
            "Asset Type": "דירה", 
            "Rooms": rooms,
            "Floor": floor,
            "Sqm": int(sqm),
            "Price": int(price),
            "Price/Sqm": price_per_sqm,
            "Share": f"{int(share*100)}%",
            "Platform": random.choice(platforms)
        })
        
    # Sort and Outlier Filter (Mock)
    df = pd.DataFrame(data)
    if not df.empty:
         df['DateObj'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
         df = df.sort_values('DateObj').drop(columns=['DateObj'])
         
         if exclude_abnormal:
             mean = df['Price/Sqm'].mean()
             std = df['Price/Sqm'].std()
             df = df[(df['Price/Sqm'] >= mean - 2*std) & (df['Price/Sqm'] <= mean + 2*std)]

    return df

# Helper to run async in sync context (for Streamlit)
def get_data(city, start, end, neighborhood=None, min_rooms=0, max_rooms=100, min_floor=-10, max_floor=200, min_sqm=0, max_sqm=10000, exclude_abnormal=False, use_mock_data=True):
    return asyncio.run(fetch_nadlan_data(
        city, start, end, neighborhood, 
        min_rooms, max_rooms,
        min_floor, max_floor,
        min_sqm, max_sqm,
        exclude_abnormal,
        use_mock_data
    ))
