#_*_coding: utf-8_*_
import zipfile
import pandas as pd
import os
from msconnect import mssql
import shutil

itzadir = os.getcwd()

for j in os.listdir('.'):
    if j[:3] == 'ENM' and j[-3:] in 'zip':
       os.remove(j)

for y in os.listdir('.'):
    if y[-3:] == 'zip':
        df=y

print(df)


print(zipfile.is_zipfile(df))

z = zipfile.ZipFile(df, 'r')

print(z)

z.printdir()

z.extractall()

x = os.listdir()

print(x)

dfx = pd.DataFrame(x)

print(dfx)

dfx.columns = ['FILENAME']

print(dfx)


if __name__ == '__main__':


    ms = mssql()
    ldap = ms.fetch('select * from ****')
    print(ldap[0])
    dfLdap = pd.DataFrame(ldap)
    print(dfLdap)

dfLdap.columns = ['node','OMC']

result = pd.merge(
    dfx, dfLdap, left_on="FILENAME", right_on="node", how="inner", sort=False)
print(result)

unique_names = pd.unique(result['OMC'])

print(unique_names)

itzadir = os.getcwd()

for zh in unique_names:
    os.makedirs(os.path.join(itzadir, zh))



for i in result.index:
    shutil.move(
        os.path.join(itzadir, result.node[i]),
        os.path.join(itzadir, result.OMC[i], result.node[i])
    )


for j in result.index:
    shutil.make_archive(os.path.join(itzadir, result.OMC[j]), 'zip',
                    itzadir,
                    result.OMC[j])






for d in os.listdir('.'):
    if d[:3] == 'ENM' and d[-3:] not in 'zip':
       shutil.rmtree(os.path.join(itzadir, d))



