import requests
from bs4 import BeautifulSoup
import datetime
import re
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA')
    )

def append_to_snowflake_raw(df, conn):
    cursor = conn.cursor()
    table_name = os.getenv('VISAS_ISSUED_BY_POST_RAW')
    cursor.execute(f"SELECT MAX(DATE) FROM {table_name}")
    max_date_in_snowflake = cursor.fetchone()[0]

    if df['DATE'].max() > max_date_in_snowflake:
        success, num_chunks, num_rows, output = write_pandas(conn, df, table_name)
        print(f"Inserted {num_rows} new rows into the raw table.")
    else:
        print("No new data to insert into raw table.")

    cursor.close()

def append_to_snowflake(df, conn):
    pass

def log_all_links(url):
    parser = 'html.parser'
    
    response = requests.get(url, verify=False)
    
    soup = BeautifulSoup(response.content, parser)
    
    links = soup.find_all('a')
    latest_date = datetime.datetime.min
    latest_excel_link = None
    
    for link in links:
        href = link.get('href')
        if href and 'pdf' in href and 'by post' in link.text.lower():
            match = re.search(r'\b(\w+)\s+(\d{4})\b', link.text)
            if match:
                month, year = match.groups()
                date_value = datetime.datetime.strptime(f'{month} {year}', '%B %Y')
                
                if date_value > latest_date:
                    latest_date = date_value
                    excel_link = link.find_next_sibling('a')
                    if excel_link and '.xlsx' in excel_link.get('href', ''):
                        latest_excel_link = 'https://travel.state.gov' + excel_link.get('href')
    
    if latest_date > datetime.datetime.min:
        print("Latest date found:", latest_date)
        print("Here is the latest Excel link:", latest_excel_link)
        df = pd.read_excel(latest_excel_link, sheet_name='Sheet1', skiprows=1)
        
        df.columns = df.columns.str.upper().str.strip().str.replace("'", "").str.replace(" ", "_")
        
        required_columns = {'POST', 'VISA_CLASS', 'ISSUANCES'}
        if not required_columns.issubset(set(df.columns)):
            print("Required columns are missing.")
            return None
        
        df['POST'] = df['POST'].str.strip()
        df['VISA_CLASS'] = df['VISA_CLASS'].str.strip()
        df['ISSUANCES'] = df['ISSUANCES'].astype(str).str.replace(',', '').astype(int)
        
        df['DATE'] = latest_date.replace(day=1).date()
        
        df = df[~df['POST'].str.strip().str.lower().eq('grand total')]
        
        return df
    
    return None

if __name__ == "__main__":
    url = "https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/nonimmigrant-visa-statistics/monthly-nonimmigrant-visa-issuances.html"
    df = log_all_links(url)
    
    if df is not None:
        conn = get_snowflake_connection()
        try:
            append_to_snowflake_raw(df, conn)
            append_to_snowflake(df, conn)
        finally:
            conn.close()
    else:
        print("No data retrieved.")