import os
import pandas as pd
from sqlalchemy import create_engine
import unidecode

user = "postgres"
password = "88776655"
host = "localhost"
port = "5432"
database = "raw_atividade2"
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

root_directory = r"C:\Users\maria\OneDrive\Documentos\atividade_2\dados"

def detect_delimiter(file_path):  #função para detectar o delimitador
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:  #ve o arquivo ignorando erros de codificacao
        first_char = file.read(1)
        if not first_char:
            print(f"O arquivo {file_path} está vazio. Pulando")
            return None #retorna None se o arquivo vazio

        file.seek(0)
        sample = []
        for _ in range(5):
            try:
                sample.append(next(file))
            except StopIteration:
                break
        sample = ''.join(sample)

        if sample.count('\t') > sample.count('|') and sample.count('\t') > sample.count(';'):  #detecta o delimitador com base no número de ocorrências nas amostras
            return '\t'
        elif sample.count('|') > sample.count('\t') and sample.count('|') > sample.count(';'):
            return '|'
        elif sample.count(';') > sample.count('\t') and sample.count(';') > sample.count('|'):
            return ';'
        else:
            raise ValueError("Delimitador desconhecido")

def clean_text(text):  #função para limpar caracteres mal decodificados
    text = unidecode.unidecode(text) #remove acentos e caracteres especiais
    return text.encode('ascii', 'ignore').decode('ascii')

for subdir, _, files in os.walk(root_directory):  #iterar sobre as pastas e arquivos
    for file_name in files:
        file_path = os.path.join(subdir, file_name)
        delimiter = detect_delimiter(file_path)

        if delimiter is None:
            continue  
        try: #tenta ler de algumas forma se toma erro
            df = pd.read_csv(file_path, delimiter=delimiter, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, delimiter=delimiter, encoding='ISO-8859-1')

        df = df.apply(lambda x: x.apply(clean_text) if x.dtype == "object" else x)  #limpar e corrigir caracteres mal decodificados

        table_name = f"raw_{os.path.splitext(file_name)[0]}" #define o nome da tabela usando o nome do arquivo
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Tabela {table_name} criada com sucesso!")
