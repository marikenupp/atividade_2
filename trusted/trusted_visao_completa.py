from sqlalchemy import create_engine, text
import pandas as pd
import os
import numpy as np

#conexão
user = "postgres"
password = "88776655"
host = "localhost"
port = "5432"
database2 = "trusted_atividade2"
engine2 = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database2}')

with engine2.connect() as conn: 
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    result = conn.execute(query)
    tables = [row[0] for row in result]
    # print(tables) # lista de tabelas da raw

table_name = 'public."trusted_enquadramentoInicia_v2"'
table_name2 = 'public.trusted_glassdoor_consolidado_join_match_less_v2'
table_name3 = 'public.trusted_glassdoor_consolidado_join_match_v2'
table_name4 = 'public.trusted_ano_tri'

query = text(f'SELECT * FROM {table_name}')
df = pd.read_sql_query(query, engine2)

query2 = text(f'SELECT * FROM {table_name2}')
df2 = pd.read_sql_query(query2, engine2)

query3 = text(f'SELECT * FROM {table_name3}')
df3 = pd.read_sql_query(query3, engine2)

query4 = text(f'SELECT * FROM {table_name4}')
df4 = pd.read_sql_query(query4, engine2)

result = pd.concat([df2, df3])

# result_left = pd.merge(df, result, on=['ds_nome', 'cd_cnpj', 'ds_segmento'], how='right')
result_left = pd.merge(df, result, on='ds_nome', how='right')
result_left_renamed = result_left.rename(columns={'cd_cnpj_x': 'cd_cnpj', 'ds_segmento_x': 'ds_segmento'})

# Definindo as colunas desejadas com os nomes atualizados
desired_columns = [
    'ds_employer_name',
    'qtd_reviews_count',
    'qtd_culture_count',
    'qtd_salaries_count',
    'qtd_benefits_count',
    'ds_employer_website',
    'ds_employer_headquarters',
    'ds_employer_founded',
    'ds_employer_industry',
    'ds_employer_revenue',
    'ds_url',
    'ds_geral',
    'ds_cultura_valores',
    'ds_diversidade_inclusao',
    'ds_qualidade_vida',
    'ds_alta_lideranca',
    'ds_remuneracao_beneficios',
    'ds_oportunidade_carreira',
    'qtd_recomenda_para_outras_pessoas',
    'qtd_perspectiva_positiva_emrpresa',
    'cd_cnpj',#nome atualizado
    'ds_nome',
    'ds_match_percent',
    'ds_segmento'#nome atualizado
]

result_only_df = result_left_renamed[desired_columns]
result_final = pd.merge(result_only_df, df4, on='ds_nome', how='inner')
# result_final.to_csv(r'C:\Users\maria\OneDrive\Documentos\atividade_2\dados_combinados.csv', index=False)

output_dir = r'C:\Users\maria\OneDrive\Documentos\atividade_2\dados_parquet' #diretorio
parquet_file = os.path.join(output_dir, 'trusted_visao_completa') #nome do arquivo

result_final.to_parquet(parquet_file, index=False) #salvar o DataFrame como um arquivo Parquet

result_final = pd.read_parquet(parquet_file) #carregar o arquivo Parquet em um DataFrame

new_table_name = 'trusted_visao_completa'

with engine2.connect() as conn:
    result_final.to_sql(new_table_name, conn, if_exists='replace', index=False, schema='public')

print(f"DataFrame carregado para a tabela {new_table_name}.")