import os
import requests
import pandas as pd
from datetime import datetime
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import traceback
import pytz
import re
import random

load_dotenv()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

BASE_URL = "https://travel.state.gov/content/travel/resources/database/database.getVisaWaitTimes.html"

# Maps API response position to visa type
VISA_TYPES = [
    "Interview Required Visitors (B1/B2)", 
    "Interview Required Students/Exchange Visitors (F, M, J)",
    "Interview Required Petition-Based Temporary Workers (H, L, O, P, Q)",
    "Interview Required Crew and Transit (C, D, C1/D)",
    "Interview Waiver Visitors (B1/B2)",
    "Interview Waiver Students/Exchange Visitors (F, M, J)",
    "Interview Waiver Petition-Based Temporary Workers (H, L, O, P, Q)",
    "Interview Waiver Crew and Transit (C, D, C1/D)"
]

CONSULATE_IDS = {
    "Abidjan": "abidjan",
    "Abu Dhabi": "P2",
    "Abuja": "P3",
    "Accra": "P4",
    "Adana": "adana",
    "Addis Ababa": "P5",
    "Algiers": "P6",
    "Almaty": "P7",
    "Amman": "P8",
    "Amsterdam": "P9",
    "Ankara": "P10",
    "Antananarivo": "P11",
    "Apia": "P225",
    "Ashgabat": "P12",
    "Asmara": "P13",
    "Astana": "astana",
    "Asuncion": "P15",
    "Athens": "athens",
    "Auckland": "P17",
    "Baghdad": "P226",
    "Baku": "P19",
    "Bamako": "P20",
    "Bandar Seri Begawan": "P21",
    "Bangkok": "P22",
    "Bangui": "bangui",
    "Banjul": "banjul",
    "Barcelona": "barcelona",
    "Beijing": "P24",
    "Beirut": "P25",
    "Belfast": "P26",
    "Belgrade": "P27",
    "Belmopan": "P28",
    "Berlin": "P29",
    "Bern": "P30",
    "Bishkek": "P31",
    "Bogota": "P32",
    "Brasilia": "P33",
    "Bratislava": "P34",
    "Brazzaville": "P35",
    "Bridgetown": "P36",
    "Brussels": "P37",
    "Bucharest": "P38",
    "Budapest": "P39",
    "Buenos Aires": "P40",
    "Bujumbura": "P41",
    "Cairo": "P42",
    "Calgary": "P43",
    "Canberra": "canberra",
    "Cape Town": "P44",
    "Caracas": "P45",
    "Casablanca": "P46",
    "Chengdu": "P47",
    "Chennai (Madras)": "P48",
    "Chiang Mai": "P49",
    "Chisinau": "P50",
    "Ciudad Juarez": "P51",
    "Colombo": "P52",
    "Conakry": "P53",
    "Copenhagen": "P54",
    "Cotonou": "P55",
    "Curacao": "P223",
    "Dakar": "P56",
    "Damascus": "P57",
    "Dar Es Salaam": "P58",
    "Dhahran": "P59",
    "Dhaka": "P60",
    "Dili": "P227",
    "Djibouti": "P61",
    "Doha": "P62",
    "Dubai": "P63",
    "Dublin": "P64",
    "Durban": "P65",
    "Dushanbe": "P66",
    "Edinburgh": "edinburgh",
    "Erbil": "erbil",
    "Florence": "P67",
    "Frankfurt": "P68",
    "Freetown": "P69",
    "Fukuoka": "fukuoka",
    "Gaborone": "P70",
    "Georgetown": "P71",
    "Guadalajara": "P72",
    "Guangzhou": "P73",
    "Guatemala City": "P74",
    "Guayaquil": "P75",
    "Halifax": "P76",
    "Hamilton": "P77",
    "Hanoi": "P78",
    "Harare": "P79",
    "Havana": "P80",
    "Helsinki": "P81",
    "Hermosillo": "P82",
    "Ho Chi Minh City": "P83",
    "Hong Kong": "P84",
    "Hyderabad": "P85",
    "Islamabad": "P86",
    "Istanbul": "P87",
    "Jakarta": "P88",
    "Jeddah": "P89",
    "Jerusalem": "P90",
    "Johannesburg": "P91",
    "Juba": "P228",
    "Kabul": "P229",
    "Kampala": "P93",
    "Kaohsiung": "kaohsiung",
    "Karachi": "P94",
    "Kathmandu": "P95",
    "Khartoum": "P96",
    "Kigali": "P97",
    "Kingston": "P98",
    "Kinshasa": "P99",
    "Kolkata": "P100",
    "Kolonia": "P101",
    "Koror": "P102",
    "Krakow": "P103",
    "Kuala Lumpur": "P104",
    "Kuwait": "P105",
    "Kyiv": "P106",
    "La Paz": "P107",
    "Lagos": "P108",
    "Lahore": "lahore",
    "Libreville": "P109",
    "Lilongwe": "P110",
    "Lima": "P111",
    "Lisbon": "P112",
    "Ljubljana": "P113",
    "Lome": "lome",
    "London": "P115",
    "Luanda": "P116",
    "Lusaka": "P117",
    "Luxembourg": "P118",
    "Madrid": "P119",
    "Majuro": "P120",
    "Malabo": "P121",
    "Managua": "managua",
    "Manama": "P123",
    "Manila": "P124",
    "Maputo": "P125",
    "Marseille": "marseille",
    "Maseru": "P126",
    "Matamoros": "P127",
    "Mbabane": "P128",
    "Melbourne": "P129",
    "Merida": "P130",
    "Mexicali Tpf": "mexicali_tpf",
    "Mexico City": "P131",
    "Milan": "P132",
    "Minsk": "P133",
    "Monrovia": "P134",
    "Monterrey": "P135",
    "Montevideo": "P136",
    "Montreal": "P137",
    "Moscow": "P138",
    "Mumbai (Bombay)": "P139",
    "Munich": "P140",
    "Muscat": "P141",
    "N'Djamena": "P142",
    "Naha": "P143",
    "Nairobi": "P144",
    "Naples": "P145",
    "Nassau": "P146",
    "New Delhi": "P147",
    "Niamey": "P148",
    "Nicosia": "P149",
    "Nogales": "P150",
    "Nouakchott": "P151",
    "Nuevo Laredo": "P152",
    "Osaka/Kobe": "P153",
    "Oslo": "P154",
    "Ottawa": "P155",
    "Ouagadougou": "P156",
    "Panama City": "P157",
    "Paramaribo": "P158",
    "Paris": "P159",
    "Perth": "P160",
    "Phnom Penh": "P161",
    "Podgorica": "P162",
    "Ponta Delgada": "ponta_delgada",
    "Port Au Prince": "P164",
    "Port Louis": "P165",
    "Port Moresby": "P166",
    "Port Of Spain": "P167",
    "Porto Alegre": "porto_alegre",
    "Prague": "P168",
    "Praia": "P169",
    "Pretoria": "pretoria",
    "Pristina": "P231",
    "Quebec": "P170",
    "Quito": "P171",
    "Rangoon": "P172",
    "Recife": "P173",
    "Reykjavik": "P174",
    "Riga": "P175",
    "Rio De Janeiro": "P230",
    "Riyadh": "P177",
    "Rome": "P178",
    "San Jose": "P179",
    "San Salvador": "P180",
    "Sanaa": "P181",
    "Santiago": "P182",
    "Santo Domingo": "P183",
    "Sao Paulo": "P184",
    "Sapporo": "P224",
    "Sarajevo": "P185",
    "Seoul": "P186",
    "Shanghai": "P187",
    "Shenyang": "P188",
    "Singapore": "P189",
    "Skopje": "P190",
    "Sofia": "P191",
    "St Georges": "st_georges",
    "St Petersburg": "P192",
    "Stockholm": "P193",
    "Surabaya": "P194",
    "Suva": "P195",
    "Sydney": "P196",
    "Taipei": "P197",
    "Tallinn": "P198",
    "Tashkent": "P199",
    "Tbilisi": "P200",
    "Tegucigalpa": "P201",
    "Tel Aviv": "P202",
    "Tijuana": "tijuana",
    "Tijuana Tpf": "P203",
    "Tirana": "P204",
    "Tokyo": "P205",
    "Toronto": "P206",
    "Tripoli": "P207",
    "Tunis": "P208",
    "Ulaanbaatar": "P209",
    "Usun-New York": "usun-new_york",
    "Valletta": "P210",
    "Vancouver": "P211",
    "Vienna": "P212",
    "Vientiane": "P213",
    "Vilnius": "P214",
    "Vladivostok": "P215",
    "Warsaw": "P216",
    "Washington Refugee Processing Center": "washington_refugeeprocessingcenter",
    "Windhoek": "P217",
    "Wuhan": "wuhan",
    "Yaounde": "P218",
    "Yekaterinburg": "yekaterinburg", 
    "Yerevan": "P220",
    "Zagreb": "P221"
}

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA')
    )

def is_weekday():
    return datetime.now(pytz.timezone('America/New_York')).weekday() < 5

def get_wait_times():
    if not is_weekday():
        logger.info("Today is not a weekday. Exiting.")
        return None

    current_date = datetime.now(pytz.timezone('America/New_York')).date()
    table_data = []

    try:
        # Randomly sample 10 consulates
        test_consulates = dict(random.sample(list(CONSULATE_IDS.items()), 10))
        
        for post, cid in test_consulates.items():
            response = requests.get(f"{BASE_URL}?cid={cid}")
            response.raise_for_status()
            wait_times = response.text.strip().split('|')
            
            logger.info(f"\nConsulate: {post} (CID: {cid})")
            logger.info(f"Raw response: {response.text.strip()}")
            
            # Create a row for every visa type for every consulate
            for visa_type, wait_time in zip(VISA_TYPES, wait_times):
                row_data = [
                    current_date,
                    post,
                    visa_type,
                    wait_time.strip()
                ]
                table_data.append(row_data)
                    
        return pd.DataFrame(table_data, columns=['DATE', 'POST', 'NONIMMIGRANT_VISA_TYPE', 'APPOINTMENT_WAIT_TIME_RAW'])

    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
    except Exception as e:
        logger.error(f"Error processing data: {e}")

    return None

def process_visa_type(visa_type):
    return visa_type.replace('Interview Required', '').strip().replace('\xa0', ' ')

def parse_appointment_wait_time(wait_time_raw):
    if wait_time_raw.lower() == 'same day':
        return 0
    days_match = re.search(r'(\d+)\s*(day|days)', wait_time_raw, re.IGNORECASE)
    return int(days_match.group(1)) if days_match else None

def determine_status(wait_time_raw, wait_time_days):
    if wait_time_raw and (wait_time_days is None or pd.isna(wait_time_days) or wait_time_days == 0):
        return wait_time_raw
    return ''

def append_to_snowflake_processed(df, conn):
    cursor = conn.cursor()
    table_name = os.environ.get('TEST_SNOWFLAKE_VISA_WAIT_TIME_TABLE')
    
    try:
        cursor.execute(f"SELECT MAX(DATE) FROM {table_name}")
        max_date = cursor.fetchone()[0]
        
        if max_date is None or df['DATE'].max() > max_date:
            processed_df = df.copy()
            processed_df['APPOINTMENT_WAIT_TIME'] = processed_df['APPOINTMENT_WAIT_TIME_RAW'].apply(parse_appointment_wait_time)
            processed_df['STATUS'] = processed_df.apply(lambda row: determine_status(row['APPOINTMENT_WAIT_TIME_RAW'], row['APPOINTMENT_WAIT_TIME']), axis=1)
            processed_df = processed_df.drop(columns=['APPOINTMENT_WAIT_TIME_RAW'])

            success, num_chunks, num_rows, output = write_pandas(conn, processed_df, table_name)
            logger.info(f"Inserted {num_rows} new rows into test table.")
        else:
            logger.info("No new data to insert into test table.")
    except Exception as e:
        logger.error(f"Error inserting data into Snowflake test table: {e}")
        print(f"Full error traceback:\n{traceback.format_exc()}")
    finally:
        cursor.close()

if __name__ == "__main__":
    conn = get_snowflake_connection()
    try:
        data_raw = get_wait_times()
        if data_raw is not None:
            append_to_snowflake_processed(data_raw, conn)
        else:
            logger.error("Failed to retrieve data.")
    finally:
        conn.close()