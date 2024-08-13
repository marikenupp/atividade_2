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

table_name = 'public.raw_glassdoor_consolidado_join_match_v2'  

query = text(f'SELECT * FROM {table_name}')
df = pd.read_sql_query(query, engine)

# print("Colunas no DataFrame:", df.columns.tolist()) #printa colunas

df = df.rename(columns={
    'employer_name': 'ds_employer_name', 
    'reviews_count': 'qtd_reviews_count', 
    'culture_count': 'qtd_culture_count', 
    'salaries_count': 'qtd_salaries_count', 
    'benefits_count': 'qtd_benefits_count', 
    'employer-website': 'ds_employer_website', 
    'employer-headquarters': 'ds_employer_headquarters', 
    'employer-founded': 'ds_employer_founded', 
    'employer-industry': 'ds_employer_industry', 
    'employer-revenue': 'ds_employer_revenue', 
    'url': 'ds_url', 
    'Geral': 'ds_geral', 
    'Cultura e valores': 'ds_cultura_valores', 
    'Diversidade e inclusão': 'ds_diversidade_inclusao', 
    'Qualidade de vida': 'ds_qualidade_vida', 
    'Alta liderança': 'ds_alta_lideranca', 
    'Remuneração e benefícios': 'ds_remuneracao_beneficios', 
    'Oportunidades de carreira': 'ds_oportunidade_carreira', 
    'Recomendam para outras pessoas(%)': 'qtd_recomenda_para_outras_pessoas', 
    'Perspectiva positiva da empresa(%)': 'qtd_perspectiva_positiva_emrpresa', 
    'Segmento': 'ds_segmento',
    'Nome': 'ds_nome',
    'match_percent': 'ds_match_percent'
})


df['ds_employer_name'] = df['ds_employer_name'].astype(str) #passa o type
df['qtd_reviews_count'] = df['qtd_reviews_count'].astype(int)
df['qtd_culture_count'] = df['qtd_culture_count'].astype(int)
df['qtd_salaries_count'] = df['qtd_salaries_count'].astype(int)
df['qtd_benefits_count'] = df['qtd_benefits_count'].astype(int)
df['ds_employer_website'] = df['ds_employer_website'].astype(str) 
df['ds_employer_headquarters'] = df['ds_employer_headquarters'].astype(str)
df['ds_employer_founded'] = pd.to_numeric(df['ds_employer_founded'], errors='coerce').fillna(0).astype(int)
df['ds_employer_industry'] = df['ds_employer_industry'].astype(str)
df['ds_employer_revenue'] = df['ds_employer_revenue'].astype(str)
df['ds_url'] = df['ds_url'].astype(str)
df['ds_geral'] = df['ds_geral'].astype(float)
df['ds_cultura_valores'] = df['ds_cultura_valores'].astype(float)
df['ds_diversidade_inclusao'] = df['ds_diversidade_inclusao'].astype(float)
df['ds_qualidade_vida'] = df['ds_qualidade_vida'].astype(float)
df['ds_alta_lideranca'] = df['ds_alta_lideranca'].astype(float)
df['ds_remuneracao_beneficios'] = df['ds_remuneracao_beneficios'].astype(float)
df['ds_oportunidade_carreira'] = df['ds_oportunidade_carreira'].astype(float)
df['qtd_recomenda_para_outras_pessoas'] = df['qtd_recomenda_para_outras_pessoas'].astype(int)
df['qtd_perspectiva_positiva_emrpresa'] = df['qtd_perspectiva_positiva_emrpresa'].astype(int)
df['ds_segmento'] = df['ds_segmento'].astype(str)
df['ds_nome'] = df['ds_nome'].astype(str)
df['ds_match_percent'] = df['ds_match_percent'].astype(int)

# # print(df.dtypes)
# # print(df)

output_dir = r'C:\Users\maria\OneDrive\Documentos\atividade_2\dados_parquet' #diretorio
parquet_file = os.path.join(output_dir, 'trusted_glassdoor_consolidado_join_match_v2') #nome do arquivo

df.to_parquet(parquet_file, index=False) #salvar o DataFrame como um arquivo Parquet

df = pd.read_parquet(parquet_file) #carregar o arquivo Parquet em um DataFrame

new_table_name = 'trusted_glassdoor_consolidado_join_match_v2'

with engine2.connect() as conn:
    df.to_sql(new_table_name, conn, if_exists='replace', index=False, schema='public')

print(f"DataFrame carregado para a tabela {new_table_name}.")