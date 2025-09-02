import yaml

# caminho com os links/caminhos para os dados a serem extraidos
endpoint_path = {
    "file": 'src/utils/endpoint.yaml', #TODO: usar caminho relativo
    "mode": 'r'
}

def read_yaml(path:str, mode:str):
    with open(path, mode) as f:
        endpoint = yaml.safe_load(f)

    return endpoint

def generate_url(endpoint_path:dict=endpoint_path): #TODO usar KWARGS
    # gerando o links a partir do yaml famil
    endpoint_params = read_yaml(endpoint_path.get("file"), endpoint_path.get("mode"))
    protocol =  endpoint_params["api"]["protocol"]
    host = endpoint_params["api"]["host"]
    # TODO iteracao na main pro caso de precisar buscar mais dados em paths ou links diferentes
    path = endpoint_params["api"]["endpoints"]["get"]["path"]
    params =  endpoint_params["api"]["endpoints"]["get"]["params"]
    url = f'{protocol}://{host}{path}'

    return url, params, endpoint_params