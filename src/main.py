from utils.yaml import generate_url
from utils.etl import extract, transform, load

# obtem a URl a partir de um arquivo de configuracao .yaml
url, params, _ = generate_url()

data_e = extract(url, params)
data_t = transform(data_e)

# TODO: carregar os arquivos .parquet no local designado
data_l = load(data_t)

# TODO: rodar em um container com python com versao 3 ou superior