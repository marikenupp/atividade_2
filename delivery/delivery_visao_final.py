from sqlalchemy import create_engine, text
import pandas as pd
import os
import numpy as np

#conexão
user = "postgres"
password = "88776655"
host = "localhost"
port = "5432"
database3 = "delivery_atividade2"
engine3 = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database3}')

output_dir = r'C:\Users\maria\OneDrive\Documentos\atividade_2\dados_parquet' #diretorio
parquet_file = os.path.join(output_dir, 'trusted_visao_completa') #nome do arquivo

result_final = pd.read_parquet(parquet_file) #carregar o arquivo Parquet em um DataFrame

new_table_name = 'delivery_visao_final'

with engine3.connect() as conn:
    result_final.to_sql(new_table_name, conn, if_exists='replace', index=False, schema='public')

print(f"DataFrame carregado para a tabela {new_table_name}.")

