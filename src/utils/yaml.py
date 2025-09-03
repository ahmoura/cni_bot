import yaml


# caminho com os links/caminhos para os dados a serem extraidos
# TODO: usar caminho relativo ao inves de absoluto
endpoint_path = {
    "file": 'src/utils/endpoint.yaml', 
    "mode": 'r'
}

# utilizada para ler o arquivo de configuracao com o endpoint
def read_yaml(path:str, mode:str)->dict:
    with open(path, mode) as f:
        endpoint = yaml.safe_load(f)
    return endpoint

# gera a url a partir do arquivo de configuracao
# TODO: iteracao na main pro caso de precisar buscar mais dados em paths ou links diferentes
def generate_url(endpoint_path:dict=endpoint_path)->(str,str,dict): #TODO usar KWARGS
    # gerando o links a partir do yaml
    endpoint_params = read_yaml(endpoint_path.get("file"), endpoint_path.get("mode"))
    protocol =  endpoint_params["api"]["protocol"]
    host = endpoint_params["api"]["host"]
    # TODO iteracao 
    path = endpoint_params["api"]["endpoints"]["get"]["path"]
    params =  endpoint_params["api"]["endpoints"]["get"]["params"]
    url = f'{protocol}://{host}{path}'
    return url, params, endpoint_params