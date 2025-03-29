import pandas as pd
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_hub_names():
    path = "C:/Users/benja/OneDrive/Bureau/UchicagoMSFM/Power_Market-1/ERCOT_names.zip"
    target_file = 'SP_List_EB_Mapping/Hub_Name_AND_DC_Ties_02212025_134801.csv'
    
    # Open the ZIP file and read the specified CSV
    with zipfile.ZipFile(path, 'r') as z:
        if target_file in z.namelist():
            with z.open(target_file) as f:
                df = pd.read_csv(f)
            return df
        else:
            raise FileNotFoundError(f"{target_file} not found in {path}")


def get_ercot_da_data(
    start_date: str,
    end_date: str,
    settlement_point: str,
    access_token: str,
    subscription_key: str,
    url: str = "https://api.ercot.com/api/public-reports/np4-190-cd/dam_stlmnt_pnt_prices"
) -> pd.DataFrame:
    
    # Convert string dates to datetime objects
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Ocp-Apim-Subscription-Key": subscription_key
    }
    
    all_data = []  
    
    chunk_start = start_dt
    
    while chunk_start <= end_dt:
        # define chunk end as chunk_start + 30 days or the end_dt, whichever is earlier
        chunk_end = min(chunk_start + timedelta(days=30), end_dt)
        
        # Build params
        params = {
            "deliveryDateFrom": chunk_start.strftime("%Y-%m-%d"),
            "deliveryDateTo": chunk_end.strftime("%Y-%m-%d"),
            "settlementPoint": settlement_point,
            "size": 2000000  # large enough to return all results within this chunk
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # raise an exception if there's an error
        
        json_data = response.json()
        
        if 'data' not in json_data or not json_data['data']:
            chunk_start = chunk_end + timedelta(days=1)
            continue
        
        fields = [field['name'] for field in json_data['fields']]
        
        chunk_df = pd.DataFrame(json_data['data'], columns=fields)
        
        all_data.append(chunk_df)
        
        chunk_start = chunk_end + timedelta(days=1)
    
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
    else:
        # If no data was retrieved, return an empty DataFrame with the known columns
        full_df = pd.DataFrame(columns=['deliveryDate', 'hourEnding', 'settlementPoint',
                                        'settlementPointPrice', 'DSTFlag'])
    
    full_df = full_df.sort_values(by="deliveryDate")
    full_df['deliveryDate'] = pd.to_datetime(full_df['deliveryDate'])
    return full_df

def get_ercot_lmp_data_30day_chunks(
    start_date: str,
    end_date: str,
    settlement_point: str,
    access_token: str,
    subscription_key: str,
    url: str = "https://api.ercot.com/api/public-reports/np6-788-cd/lmp_node_zone_hub"
) -> pd.DataFrame:

    # Convert start_date and end_date to datetime, defaulting to 00:00:00 and 23:59:59
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(hours=23, minutes=59, seconds=59)
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Ocp-Apim-Subscription-Key": subscription_key
    }
    
    all_data = []
    
    # We'll fetch data in ~30-day chunks until we pass end_dt
    chunk_start = start_dt
    
    while chunk_start <= end_dt:
        # Define the chunk end (30 days after chunk_start)
        chunk_end = chunk_start + timedelta(days=30)
        if chunk_end > end_dt:
            chunk_end = end_dt
        
        # Format SCEDTimestampFrom/To as 'YYYY-MM-DDTHH:mm:ss'
        sced_from = chunk_start.strftime("%Y-%m-%dT%H:%M:%S")
        sced_to   = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
        
        params = {
            "SCEDTimestampFrom": sced_from,
            "SCEDTimestampTo"  : sced_to,
            "settlementPoint"  : settlement_point,
            "size"             : 2000000
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an HTTPError if 4xx/5xx
        
        json_data = response.json()
        
        # 'fields' typically contains metadata describing columns
        if 'fields' in json_data:
            fields = [field['name'] for field in json_data['fields']]
        else:
            # Fallback if 'fields' is not returned
            fields = ["SCEDTimestamp", "settlementPoint", "LMP"]
        
        # 'data' holds the actual rows
        if 'data' in json_data and json_data['data']:
            chunk_df = pd.DataFrame(json_data['data'], columns=fields)
            all_data.append(chunk_df)
        
        # Move to the next chunk (1 second after chunk_end)
        # so we don't double-count the final second of each chunk
        chunk_start = chunk_end + timedelta(seconds=1)
    
    # Combine all chunks into one DataFrame
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
    else:
        full_df = pd.DataFrame(columns=fields)

    # Optionally, sort by SCEDTimestamp to ensure chronological order
    # (only if that column exists)
    if "SCEDTimestamp" in full_df.columns:
        full_df = full_df.sort_values(by="SCEDTimestamp").reset_index(drop=True)
    
    return full_df