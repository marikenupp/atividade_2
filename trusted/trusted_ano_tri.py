from sqlalchemy import create_engine, text
import pandas as pd
import os
import numpy as np

#conexão
user = "postgres"
password = "88776655"
host = "localhost"
port = "5432"
database = "raw_atividade2"
database2 = "trusted_atividade2"
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
engine2 = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database2}')


def process_table(table_name): #função para ler e processar uma tabela
    # print(f"{table_name}")
    query = text(f'SELECT * FROM {table_name}')
    df = pd.read_sql_query(query, engine)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df

with engine.connect() as conn:
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'raw_%_tri_%'")
    result = conn.execute(query)
    tables = [row[0] for row in result]


dataframes = [process_table(f'public."{table}"') for table in tables] #ler e processar todas as tabelas

combined_df = pd.concat(dataframes, ignore_index=True)

combined_df = combined_df.rename(columns={
    'Ano': 'ds_ano', 
    'Trimestre': 'ds_trimestre', 
    'Categoria': 'ds_categoria', 
    'Tipo': 'ds_tipo', 
    'CNPJ IF': 'cd_cnpj', 
    'Instituição financeira': 'ds_nome', 
    'Índice': 'ds_indice', 
    'Quantidade de reclamações reguladas procedentes': 'qtd_reclamacoes_reguladas_procedentes', 
    'Quantidade de reclamações reguladas - outras': 'qtd_reclamacoes_reguladas_outras', 
    'Quantidade de reclamações não reguladas': 'qtd_reclamacoes_nao_reguladas', 
    'Quantidade total de reclamações': 'qtd_total_reclamacoes', 
    'Quantidade total de clientes \x96 CCS e SCR': 'qtd_total_clientes_ccs_scr', 
    'Quantidade de clientes \x96 CCS': 'qtd_clientes_ccs', 
    'Quantidade de clientes \x96 SCR': 'qtd_clientes_scr'
})

combined_df['ds_ano'] = combined_df['ds_ano'].astype(int) #passa o type
combined_df['ds_trimestre'] = combined_df['ds_trimestre'].replace('o', '', regex=True).astype(int) 
combined_df['ds_categoria'] = combined_df['ds_categoria'].astype(str) 
combined_df['ds_tipo'] = combined_df['ds_tipo'].astype(str) 
combined_df['cd_cnpj'] = combined_df['cd_cnpj'].astype(str) 
combined_df['ds_nome'] = combined_df['ds_nome'].str.replace(' (conglomerado)', '', regex=False).astype(str) 
combined_df['ds_indice'] = combined_df['ds_indice'].replace(' ', np.nan).astype(str)
combined_df['qtd_reclamacoes_reguladas_procedentes'] = combined_df['qtd_reclamacoes_reguladas_procedentes'].astype(int) 
combined_df['qtd_reclamacoes_reguladas_outras'] = combined_df['qtd_reclamacoes_reguladas_outras'].astype(int) 
combined_df['qtd_reclamacoes_nao_reguladas'] = combined_df['qtd_reclamacoes_nao_reguladas'].astype(int) 
combined_df['qtd_total_reclamacoes'] = combined_df['qtd_total_reclamacoes'].astype(int) 
combined_df['qtd_total_clientes_ccs_scr'] = pd.to_numeric(combined_df['qtd_total_clientes_ccs_scr'], errors='coerce').fillna(0).astype(int)
combined_df['qtd_clientes_ccs'] = pd.to_numeric(combined_df['qtd_clientes_ccs'], errors='coerce').fillna(0).astype(int)
combined_df['qtd_clientes_scr'] = pd.to_numeric(combined_df['qtd_clientes_scr'], errors='coerce').fillna(0).astype(int)

# print(combined_df.dtypes)
# print(combined_df)

output_dir = r'C:\Users\maria\OneDrive\Documentos\atividade_2\dados_parquet' #diretorio
parquet_file = os.path.join(output_dir, 'trusted_ano_tri') #nome do arquivo

combined_df.to_parquet(parquet_file, index=False) #salvar o DataFrame como um arquivo Parquet

combined_df = pd.read_parquet(parquet_file) #carregar o arquivo Parquet em um DataFrame

new_table_name = 'trusted_ano_tri'

with engine2.connect() as conn:
    combined_df.to_sql(new_table_name, conn, if_exists='replace', index=False, schema='public')

print(f"DataFrame carregado para a tabela {new_table_name}.")