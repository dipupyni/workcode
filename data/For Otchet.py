#_*_coding: utf-8_*_
import pandas as pd
from datetime import datetime

myway = '****'
df0 = pd.read_excel('****',sheet_name='****')
#df0.set_index('Scrt', inplace=True)


from sqlalchemy.sql import text
import urllib
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
connection_url = URL.create(
    "mssql+pyodbc",
    username="****",
    password='****',
    host='****',
    port=****,
    database="****",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "TrustServerCertificate": "yes",
        #"authentication": "ActiveDirectoryIntegrated",
    },
)
engine = create_engine(connection_url)

with engine.connect() as conn:
    r = df0.to_sql('****', conn, schema='dbo', if_exists='replace', chunksize=1)
    print(r)

current_datetime = datetime.now().date()
splitdate = []
for sp in str(current_datetime).split('-'):
    splitdate.append(sp)
splitfinaldt=str(splitdate[2])+'.'+str(splitdate[1])+'.'+str(splitdate[0])
nameiueu='****'+splitfinaldt+'****'

df_raw = pd.read_excel(nameiueu, engine='****',sheet_name='****')
df=df_raw[['****','****']]

with engine.connect() as conn:
    r = df.to_sql('****', conn, schema='****', if_exists='replace', chunksize=1)
    print(r)

print('Finish')