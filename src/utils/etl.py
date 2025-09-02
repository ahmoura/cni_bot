from requests import get

def extract(url:str, params:dict):
    response = get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'ERROR: URL RETURN CODE {response.status_code}')
        return None
    
def transform(data:dict):
        return data