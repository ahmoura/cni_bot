from requests import get
import pandas as pd
from uuid import uuid4
from datetime import datetime

def extract(url:str, params:dict)->dict:
    """
    Extrai dados de uma URL, tratando possiveis erros de requisicao.

    Esta funcao faz uma requisicao HTTP GET para a URL fornecida
    e retorna o conteudo JSON da resposta em caso de sucesso.

    :param url: A URL para a qual a requisicao sera feita.
    :type url: str
    :param params: Parametros para a requisicao.
    :type params: dict
    :return: Um dicionario contendo a resposta JSON e o codigo de status HTTP.
    :rtype: dict
    """
    try:
        response = get(url, params=params, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"ERROR: {e}")
        return {"response": e,
                "status_code": response.status_code}
    else:
        return {"response": response.json(),
            "status_code": response.status_code}

    

 
def transform_by_type(data:dict, src_key:str = None):
    """
    Transforma dados aninhados de um dicionario em DataFrames do Pandas.

    Esta e uma funcao recursiva que percorre um dicionario aninhado.
    Se um valor for um dicionario, a funcao chama a si mesma. 
    Se for uma lista, ela cria um DataFrame e o armazena. 
    Se for um tipo de dado simples, armazena o valor para ser transformado em DataFrame no final da recursao.

    :param data: O dicionario contendo os dados a serem transformados.
    :type data: dict
    :param src_key: Chave da camada superior para nomear as chaves no dicionario de saida.
    :type src_key: str, opcional
    :return: Um dicionario onde as chaves sao nomes de tabelas e os valores sao DataFrames do Pandas.
    :rtype: dict

    :TODOS:
        - Passar todos parametros fixos (como o 'ibge' hardcoded) para o arquivo config/params e ler eles usando as funcoes do yaml
        - Efetuar outras transformacoes antes de salvar no .parquet, e.g. o campo "territorio" criou diversos arquivos com apenas uma linha. 
            Apos a criacao de todos DFs, torna-los uma tabela unica com uma coluna identificando o df source.
    """
    data_t = {}
    data_t_output = {}

    for data_key in data.keys():

        key = f'{src_key}_{data_key}' if src_key else data_key

        if isinstance(data[data_key], dict) is True:
            df_tmp = transform_by_type(data[data_key], src_key=key)
            data_t_output.update(df_tmp)

        elif isinstance(data[data_key], list) is True:
            df = pd.DataFrame(data[data_key])
            data_t_output[key] = df

        elif isinstance(data[data_key], list) is False:
            data_t[key] = data[data_key]

    df = pd.DataFrame(data_t, index=[0])

    key = 'ibge' if src_key is None else key
    data_t_output[key] = df

    return data_t_output


def transform(data:dict):
    """
    Inicia o processo de transformacao de dados aninhados para DataFrames.

    Esta funcao atua como um wrapper para a função `transform_by_type`,
    fazendo a chamada inicial para converter a estrutura de dados complexa
    em um dicionario de DataFrames.

    :param data: O dicionario com os dados a serem transformados.
    :type data: dict
    :return: Um dicionario onde as chaves sao nomes de tabelas e os valores
             sao DataFrames do Pandas.
    :rtype: dict
    """   

    data_t = transform_by_type(data)

    return data_t

# gera os metadados, adicina em cada linha que foi listada e salva em um arquivo parquet
# uma chave unica de ingestao e data tambem sao criados para ter um controle dos itens que foram transformados juntos
    # acho que o nome ingestao seria mais apropriado na etapa do extract, a chave em questao aqui seria para relacionar entre os diversos arquivos parquet para fim de joins
def load(data:dict):
    """
   Salva como arquivos Parquet.

    Esta funcao itera sobre um dicionario de DataFrames, adiciona metadados
    como um ID de ingestao e a data/hora da execucao. Em seguida, salva cada
    DataFrame em um arquivo Parquet no diretorio de saída.

    :param data: Um dicionario onde as chaves sao nomes de tabelas e os valores
                 sao DataFrames do Pandas.
    :type data: dict
    :raises Exception: Qualquer excecao levantada pela funcao `to_parquet`.

    :TODOS:
        - Mandar o tipo de arquivo por parametro (parquet, csv, etc)
        - Passar parametros fixos (como filepath) para o arquivo config/params e ler eles usando as funcoes do yaml
    """

    ingestion_id = str(uuid4())
    ingestion_date = datetime.now().isoformat()
    metadata = {"ingestion_id": ingestion_id, "ingestion_date": ingestion_date}
    df_metadata = pd.DataFrame(metadata, index=[0])

    for k, v in data.items():
        #TODO to params
        output_path = f'output/{k}.parquet'
        df = pd.concat([v, df_metadata], axis=1)
        df.to_parquet(output_path, index=False)