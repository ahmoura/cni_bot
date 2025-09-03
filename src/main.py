from utils.yaml import generate_url
from utils.etl import extract, transform

url, params, _ = generate_url()

data_e = extract(url, params)
data_t = transform(data_e)
#print(data_t["Periodos"])