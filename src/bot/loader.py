import yaml


# caminho com os links/caminhos para os dados a serem extraidos
# TODO: usar caminho relativo ao inves de absoluto
endpoint_path = {
    "file": 'config/endpoint.yaml', 
    "mode": 'r'
}

# utilizada para ler o arquivo de configuracao com o endpoint
def read_yaml(path:str, mode:str='r')->dict:   
    """
    Le e carrega dados de um arquivo YAML.

    A funcao abre um arquivo YAML no caminho especificado e o carrega
    com seguranca para um dicionario Python. Util para ler arquivos
    de configuracao.

    :param path: O caminho para o arquivo YAML a ser lido.
    :type path: str
    :param mode: O modo de abertura do arquivo.
    :type mode: str, opcional.
    :return: Um dicionario contendo os dados do arquivo YAML.
    :rtype: dict
    """
    with open(path, mode) as f:
        endpoint = yaml.safe_load(f)
    return endpoint

# gera a url a partir do arquivo de configuracao
# TODO: iteracao na main pro caso de precisar buscar mais dados em paths ou links diferentes
def generate_url(endpoint_path:dict=endpoint_path)->(str,str,dict):    
    """
    Gera uma URL de uma API a partir de um arquivo YAML.

    Esta funcao le um arquivo de configuração YAML para extrair o protocolo,
    host, caminho e parâmetros de uma API. Ela constroi a URL completa e
    retorna a URL, os parametros e o dicionario de configuracao completo.

    :param endpoint_path: Dicionario contendo o caminho e o modo para o arquivo YAML.
                          O padrao e usar a variáavel global `endpoint_path`.
    :type endpoint_path: dict, opcional
    :return: Uma tupla contendo a URL construida, os parametros da API e o dicionario
             completo de configuracao.
    :rtype: tuple[str, dict, dict]

    :TODOS:
        - Retornar dicionario
        - Iteracao na main para buscar mais links ou paths, caso existam.
    """

    endpoint_params = read_yaml(endpoint_path.get("file"), endpoint_path.get("mode"))
    protocol =  endpoint_params["api"]["protocol"]
    host = endpoint_params["api"]["host"]
     
    path = endpoint_params["api"]["endpoints"]["get"]["path"]
    params =  endpoint_params["api"]["endpoints"]["get"]["params"]
    url = f'{protocol}://{host}{path}'
    return url, params, endpoint_params