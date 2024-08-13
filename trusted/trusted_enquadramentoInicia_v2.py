from sqlalchemy import create_engine, text
import pandas as pd
import os

#conexão
user = "postgres"
password = "88776655"
host = "localhost"
port = "5432"
database = "raw_atividade2"
database2 = "trusted_atividade2"
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
engine2 = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database2}')


with engine.connect() as conn: 
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    result = conn.execute(query)
    tables = [row[0] for row in result]
    # print(tables) #lista de tabelas da raw

table_name = 'public."raw_enquadramentoInicia_v2"'  

query = text(f'SELECT * FROM {table_name}')
df = pd.read_sql_query(query, engine)

# print("Colunas no DataFrame:", df.columns.tolist()) #printa colunas

df = df.rename(columns={
    'Segmento': 'ds_segmento',  
    'CNPJ': 'cd_cnpj',          
    'Nome': 'ds_nome'           
})

df['ds_segmento'] = df['ds_segmento'].astype(str) #passa o type
df['cd_cnpj'] = df['cd_cnpj'].astype(str) 
df['ds_nome'] = df['ds_nome'].astype(str).str.replace(' - PRUDENCIAL', '', regex=False)

# print(df.dtypes)
# print(df)

output_dir = r'C:\Users\maria\OneDrive\Documentos\atividade_2\dados_parquet' #diretorio
parquet_file = os.path.join(output_dir, 'trusted_enquadramentoInicia_v2') #nome do arquivo

df.to_parquet(parquet_file, index=False) #salvar o DataFrame como um arquivo Parquet

df = pd.read_parquet(parquet_file) #carregar o arquivo Parquet em um DataFrame

new_table_name = 'trusted_enquadramentoInicia_v2'

with engine2.connect() as conn:
    df.to_sql(new_table_name, conn, if_exists='replace', index=False, schema='public')

print(f"DataFrame carregado para a tabela {new_table_name}.")