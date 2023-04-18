#_*_coding: utf-8_*_
import pandas as pd
import os
from sqlalchemy.sql import text
import urllib
from sqlalchemy.sql import text
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

connection_url2 = URL.create(
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
engine2 = create_engine(connection_url2)

resultset = pd.read_sql_query('SELECT * from ****', engine)
resultset2 = pd.read_sql_query('SELECT * from ****', engine)
resultset3 = pd.read_sql_query("select * from ****", engine)
resultset4 = pd.read_sql_query('SELECT * from ****', engine2)
resultset5 = pd.read_sql_query('SELECT * from ****', engine)

for j in os.listdir('****'):

    if j == '****':
       print(j)
       os.remove(os.path.join('****', j))


with pd.ExcelWriter('****') as writer:
    resultset.to_excel(writer, sheet_name='****',index=False)
    resultset2.to_excel(writer, sheet_name='****', index=False)
    resultset3.to_excel(writer, sheet_name='****', index=False)
    resultset4.to_excel(writer, sheet_name='****', index=False)
    resultset5.to_excel(writer, sheet_name='****', index=False)

print('Finish')