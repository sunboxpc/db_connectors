# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 15:18:35 2019

@author: Sergey.Glukhov
"""

from psycopg2 import sql
from sqlalchemy import create_engine
import pandas as pd
from getpass import getpass
#import cx_Oracle
from sqlalchemy import text

#прописываем пути к библиотекам oracle instanclient
"""
import os
import platform

LOCATION = r"c:\Oracle\instantclient_19_5"
print("ARCH:", platform.architecture())
print("FILES AT LOCATION:")
for name in os.listdir(LOCATION):
    print(name)
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
"""

#чтение sql запроса из файла
def get_sql(file_name):
    with open(file_name, 'r', encoding='utf-8', newline='') as file:
        sql = file.read()
        sql = text(sql)
    return sql

# создание подключения к Postgres
def my_postgres_conncection(
        db_name, 
        server='localhost', 
        user='postgres',
        port = '5432'):

    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
            user, 
            getpass(),
            server,
            port, 
            db_name))
    return engine 

# создание подключения к Oracle
def oracle_cso_connection(
        user_oracle,
        port = '1521',
        host = 'ECODWH.UR.RT.RU', #'10.184.64.28'
        service_name = 'ecodwh.ur.rt.ru'):
    
#    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    engine =  create_engine(
        'oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}'.format(
            user=user_oracle,
            password=getpass(),
            host=host,
            port=port,
            service_name=service_name
        ),
        convert_unicode=False,
        pool_recycle=10,
        pool_size=50,
        echo=True,
        connect_args = {'encoding': 'UTF-8'}
    )
    return engine

#выполнение sql запроса
def query(sql, engine):
    with engine.begin() as con:  
        return con.execute(text(sql))
    
   
# загрузка данных в базу с помощью Pandas   
    
def write_multy_dfs(engine, name, dfs, mode='replace'):

    for i, df in enumerate(dfs):
        if mode == 'replace' and i == 0:
            mode_actual = 'replace'
        else: mode_actual = 'append'
        print('upload to db file {}'.format(df['file'].unique()[0]))
        df.to_sql(name, engine, if_exists=mode_actual, index=False)

#выгрузка данных из базы в файл 

def sql_to_excel(sql, engine, file_name):
    df = pd.read_sql(sql, engine)
    try:
        df.to_excel(file_name)
    except: pass
    return df
    
    
    
    
    
    
    


#con = psycopg2.connect(dbname='postgres',
#      user='postgres', host='',
#      password='******')
#
#con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
#
#cur = con.cursor()
#db_name = 'clie_db'
# Use the psycopg2.sql module instead of string concatenation 
# in order to avoid sql injection attacs.
#cur.execute(sql.SQL("CREATE DATABASE {}").format(
#        sql.Identifier(db_name))
#    )
#con.commit()
#cur.close()
#con.close()

    

#con = cx_Oracle.connect('{user}/{password}@{host}:{port}/{service_name}'.format(
#        user=user_oracle,
#        password=getpass(),
#        host=host,
#        port=port,
#        service_name=service_name ), encoding='UTF-8')

if __name__ == '__main__':
    df = pd.read_sql('select * from crs.cso_connects where period in (201901)', engine)
    engine = my_postgres_conncection()
    df = extract_sql(get_sql('sql/join_agent_agent2_esed.sql'), engine, 'join_agent_agent2_esed.xlsx')
    df = extract_sql(get_sql('sql/join_agent_agent2_esed_pivot.sql'), engine, 'join_agent_agent2_esed_pivot2.xlsx')
    df = extract_sql(get_sql('sql/load_esed.sql'), engine, 'esed.xlsx')
    
    
    #db = sql.SQLDatabase(engine)
    #t = sql.SQLTable('test', db, frame=df, if_exists='replace')
    #for c in t.table.columns: c.quote = False
    #
    #for c in t.table.columns:
    #    c.quote = False
    #
    #t.create()
    #t.insert()
    
    df.columns = df.columns.str.lower()
    
    df.to_sql('fiopro_db', engine, if_exists='replace')
    sql = 'ALTER TABLE {} ADD PRIMARY KEY (id)'
    query(sql.format('fiopro_db'), engine)
    #with engine.begin() as con: con.execute('ALTER TABLE {} ADD PRIMARY KEY (id)'.format('fiopro_db'))
    df.to_sql('temp_table', engine, if_exists='replace')
    query(sql.format('temp_table'), engine)
    #with engine.begin() as con: con.execute('ALTER TABLE {} ADD PRIMARY KEY (id)'.format('temp_table'))
    clie_db.columns = clie_db.columns.str.lower()
    clie_db.index.name = 'id_sale'
    clie_db.to_sql('scource', engine, if_exists='replace')
    debt = pd.read_excel('debts.xlsx', sheet_name ='Table1')
    debt.columns = debt.columns.str.lower()
    debt.index.name = 'id_debt'
    debt.to_sql('debt', engine, if_exists='replace')
    
    
    file_name = 'sql/update_or_insert_fiopro_db.sql'
    sql ="""
    select column_name,data_type 
    from information_schema.columns 
    where table_name = '{}';
    """
    
    
    #col_df = pd.read_sql(sql.format('scource') ,engine)
    #col_df.to_excel('col.xlsx')
    table_name = 'scource'
    clie_db.to_sql(table_name, engine, if_exists='replace')
    sql_stat = 'ALTER TABLE {} \n'.format(table_name)
    sql_result = sql_stat
    for column_name, column_type in column_type_dict.items():
        sql_var = 'ALTER COLUMN "{}" TYPE {},\n'.format(column_name, column_type)
        sql_result += sql_var
        
    query(sql_result[:-2], engine)
    
    
    file_name = 'sql/agent_remuneration_v4.sql'
    with open(file_name, 'r', encoding='utf-8', newline='') as file:
                    paid_sql = file.read()
    
    
    paid2 = pd.read_sql(paid_sql.replace('%', '%%'), engine)
    paid2.to_excel('agent3.xlsx', index= False)








