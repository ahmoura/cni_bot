from src.bot.loader import generate_url
from src.bot.etl import extract, transform, load

# obtem a URl a partir de um arquivo de configuracao .yaml
url, params, _ = generate_url()

data_e = extract(url, params)

if data_e and data_e.get("status_code") == 200:  
    data_t = transform(data_e.get("response"))
    data_l = load(data_t)

# TODO: rodar em um container com python com versao 3 ou superior


#TODO MOVE TO FUNCTION
import pandas as pd
import os
for f in os.listdir('./output/'):
    print(pd.read_parquet(f'./output/{f}'))