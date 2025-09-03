from requests import get
import pandas as pd
from uuid import uuid4
from datetime import datetime

# funcao para extrair os dados da url lida do arquivo de configuracao
# TODO: usar try except ao inves de printar um erro
def extract(url:str, params:dict):
    response = get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'ERROR: URL RETURN CODE {response.status_code}')
        return None


# funcao recursiva baseada no tipo de dado que foi lido no payload
# cada chamada recursiva passa o nome da chave fonte para refenciar no arquivo de saida
# se for um dicionario, recursao
# se for uma lista, salva um arquivo parquet no modo tabular
# caso contrario, armazena em uma lista (aqui dict) para salvar ao final da execucao
# uma chave unica de ingestao e data tambem sao criados para ter um controle dos itens que foram transformados juntos
    # acho que o nome ingestao seria mais apropriado na etapa do extract, a chave em questao aqui seria para relacionar entre os diversos arquivos parquet para fim de joins
# TODO: poderia ser feito um trabalho mais eficiente na validacao dos dados, por exemplo os dados do campo "territorio", diversos arquivos foram criados com apenas uma linha. Nessa etapa ou apos a criacao de todos eles um tratamento para torna-los uma tabela unica com uma coluna identificando a key seria mais eficiente e organizado.
def transform_by_type(metadata:pd.DataFrame, data:dict, src_key:str = None):
    data_t = {}
    for data_key in data.keys():

        k = f'{src_key}_{data_key}' if src_key is not None else data_key
        output_path = f'output/{k}.parquet'
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

# faz o controle dos metadados e invoca a primeira chamada da funcao recursiva que atua sobre os tipos de dados
def transform(data:dict):
        
        ingestion_id = str(uuid4())
        ingestion_date = datetime.now().isoformat()
        metadata = {"ingestion_id": ingestion_id, "ingestion_date": ingestion_date}
        df_metadata = pd.DataFrame(metadata, index=[0])

        transform_by_type(df_metadata, data)