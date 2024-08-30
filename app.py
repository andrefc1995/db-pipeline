import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime
import logging

load_dotenv()
logger = logging.getLogger(__name__)

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

def classifica_hora(hra):
    if 0 <= hra < 6: return "MADRUGADA"
    elif 6 <= hra < 12: return "MANHA"
    elif 12 <= hra < 18: return "TARDE"
    else: return "NOITE"

def flg_status(atraso):
    if atraso > 0.5 : return "ATRASO"
    else: return "ONTIME"

def feat_eng(df):
    """
    Função para criação de novos campos no DataFrame.
    INPUT: Pandas DataFrame
    OUTPUT: Pandas DataFrame com novas features
    """
    df["tempo_voo_esperado"] = (df["datetime_chegada_formatted"] - df["datetime_partida_formatted"]) / pd.Timedelta(hours=1)
    df["tempo_voo_hr"] = df["tempo_voo"] /60
    df["atraso"] = df["tempo_voo_hr"] - df["tempo_voo_esperado"]
    df["dia_semana"] = df["data_voo"].dt.day_of_week
    df["horario"] = df.loc[:,"datetime_partida_formatted"].dt.hour.apply(lambda x: classifica_hora(x))
    df["flg_status"] = df.loc[:,"atraso"].apply(lambda x: flg_status(x))
    
    logger.info(f'Engenharia de features concluída; {datetime.datetime.now()}')
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
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()

if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados  = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'),index_col=0)
    df = data_clean(df, metadados)
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves"])
    df = feat_eng(df)
    save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')