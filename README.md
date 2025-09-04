# Robô de Captura de Dados do IPCA (CNI-BOT)

Este projeto é um MVP de um bot desenvolvido em Python para capturar dados do IBGE.

O objetivo principal deste bot é automatizar a extração dos dados do IPCA de uma URL específica, garantindo que o conteúdo seja transformado em um formato tabular e, por fim, armazenado para posterior análise.

## Como Executar

1.  **Pré-requisitos:** Certifique-se de ter as bibliotecas Python necessárias instaladas (`pandas`, `requests`, `pyarrow`, `uuid`, etc.).
2.  **Execução:** Execute o script principal para iniciar o processo de ETL. O bot irá capturar os dados, transformá-los em tabelas e salvá-las no diretório `output/` como arquivos Parquet.