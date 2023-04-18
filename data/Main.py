# -*- coding: utf-8
from msconnect import mssql
import pandas as pd
import os


for x in os.listdir('.'):
    if x[:] == 'add.xls':
        os.remove(x)

for y in os.listdir('.'):
    if y[-3:] == 'xls':
        df = pd.read_excel(y)

print(df)

for w in df.columns:
    w

df.loc[1000000] = w

print(df)

for z in df.head():
    df[z]

df1 = pd.DataFrame(
    {
         '****': df[z]

    },
    index=df.index
)


print(df1)

if __name__ == '__main__':


    ms = mssql()
    ldap = ms.fetch('select * from ****)
    print(ldap[0])
    dfLdap = pd.DataFrame(ldap)
    print(dfLdap)

dfLdap.columns = ['node','OMC']

result = pd.merge(
    df1, dfLdap, left_on="BTS_Name", right_on="node", how="left", sort=False)

print(result)

df2 = pd.DataFrame(
    {
         'NE': result['BTS_Name'],
        'ENM': result['OMC'],

    },
    index=result.index
)

print(df2)

df2.set_index('NE', inplace=True)

df2.index.name = 'NE'
df2.to_excel('add.xls')