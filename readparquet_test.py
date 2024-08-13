import pandas as pd

parquet_file = r"C:\Users\maria\OneDrive\Documentos\atividade_2\dados_parquet\trusted_ano_tri"

df = pd.read_parquet(parquet_file)

print(df)
