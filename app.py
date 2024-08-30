import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime

load_dotenv()

def data_clean(df, metadados):
    '''
    Função principal para saneamento dos dados
    INPUT: Pandas DataFrame, dicionário de metadados
    OUTPUT: Pandas DataFrame, base tratada
    '''
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])

    df.loc[:,"datetime_partida"] = df.loc[:,"datetime_partida"].str.replace('.0', '')
    df.loc[:,"datetime_chegada"] = df.loc[:,"datetime_chegada"].str.replace('.0', '')

    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:,col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df

def feat_eng(df):
    '''
    Função para criar novas features no DataFrame.
    INPUT: DataFrame original.
    OUTPUT: DataFrame com novas features adicionadas.
    '''
    df['tempo_voo_esperado'] = (df['datetime_chegada'] - df['datetime_partida']).dt.total_seconds() / 3600
    df['tempo_voo_hr'] = df['tempo_voo_esperado'].apply(lambda x: np.floor(x))
    df['atraso'] = (df['tempo_voo_esperado'] - df['scheduled_time']).apply(lambda x: x if x > 0 else 0)
    df['flg_status'] = df['atraso'].apply(lambda x: 'atrasado' if x > 0 else 'no_horario')
    df['dia_semana'] = df['datetime_partida'].dt.day_name()
    df['horario'] = df['datetime_partida'].dt.hour

    logger.info(f"Engenharia de features concluída; {datetime.datetime.now()}")
    return df

def save_data_sqlite(df):
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except Exception as e:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()} - Erro: {e}')
        return
    c = conn.cursor()
    df.to_sql('nyflights', con=conn, if_exists='replace', index=False)
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()

def fetch_sqlite_data(table):
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
        c = conn.cursor()
        c.execute(f"SELECT * FROM {table} LIMIT 5")
        records = c.fetchall()
        print("Primeiros registros da tabela:")
        for record in records:
            print(record)
        conn.commit()
    except Exception as e:
        logger.error(f'Problema na conexão com banco ou ao buscar dados; {datetime.datetime.now()} - Erro: {e}')
    finally:
        conn.close()

if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    
    metadados  = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'), index_col=0)
    
    # Limpeza de dados
    df = data_clean(df, metadados)

    # Validação de nulos e chaves
    utils.null_check(df, metadados["null_tolerance"])
    keys_valid = utils.keys_check(df, metadados["cols_chaves"])
    if not keys_valid:
        logger.error('Erro na validação de chaves, processo encerrado.')
        exit()

    # Engenharia de features
    df = feat_eng(df)

    # Salvamento no SQLite
    save_data_sqlite(df)
    
    # Exibição dos primeiros registros da tabela
    fetch_sqlite_data(metadados["tabela"][0])
    
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')
