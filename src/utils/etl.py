from requests import get
import pandas as pd
from uuid import uuid4
from datetime import datetime


def extract(url:str, params:dict):
    response = get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'ERROR: URL RETURN CODE {response.status_code}')
        return None

def transform_by_type(metadata:pd.DataFrame, data:dict, src_key:str = None):

    data_t = {}

    for data_key in data.keys():

        k = f'{src_key}_{data_key}' if src_key is not None else data_key
        output_path = f'output/{k}.parquet'

        #print(f'DATAKEY: {data_key} {type(data[data_key])}')
        if isinstance(data[data_key], dict) is True:
            transform_by_type(metadata, data[data_key], src_key=k)

        elif isinstance(data[data_key], list) is True:
            df = pd.DataFrame(data[data_key])
            df = pd.concat([df, metadata], axis=1)
            df.to_parquet(output_path, index=False)
        elif isinstance(data[data_key], list) is False:
            data_t[k] = data[data_key]
    df = pd.DataFrame(data_t, index=[0])
    df = pd.concat([df, metadata], axis=1)
    df.to_parquet(output_path, index=False)

def transform(data:dict):
        
        ingestion_id = str(uuid4())
        ingestion_date = datetime.now().isoformat()
        metadata = {"ingestion_id": ingestion_id, "ingestion_date": ingestion_date}
        df_metadata = pd.DataFrame(metadata, index=[0])

        transform_by_type(df_metadata, data)