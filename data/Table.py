#_*_coding: utf-8_*_
import pandas as pd
import os
from msconnect import mssql
import sqlalchemy
from datetime import datetime
from sqlalchemy.orm import sessionmaker

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', -1)

#print(os.listdir(r'\\10.40.189.9\License_Installation'))

filepath = '****'

#print(filepath)



for y in os.listdir('****'):
    if y == '****':
        df = pd.read_excel(os.path.join(filepath, y))

#df.set_index('Дата', inplace=True)

#print(df)

filepath_home = '****'

current_datetime = datetime.now().date()
current_datetime_morning = (f'****')
current_datetime_day = (f'****')
current_datetime_evening = (f'****')

#print(current_datetime_evening)

#print(os.listdir(filepath_home))

listdir = [d for d in os.listdir(filepath_home) if d.endswith(".xls") and f'****' in d]

for o in os.listdir('****'):
    if o == '****':
        df2 = pd.read_excel(os.path.join(filepath, o), 'Установлено')

#print(df2)

salary_sheets = {'На установку': df, 'Установлено': df2}

#print (salary_sheets['Установлено'])

if len(listdir) == 0:
    writer = pd.ExcelWriter(os.path.join(filepath_home, current_datetime_morning))
    for sheet_name in salary_sheets.keys():
        salary_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()
    exit()

for j in listdir:
    if j[:] == current_datetime_morning and current_datetime_day not in listdir:
       writer = pd.ExcelWriter(os.path.join(filepath_home, current_datetime_day))
       for sheet_name in salary_sheets.keys():
           salary_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
       writer.save()
    elif j[:] == current_datetime_day and current_datetime_evening not in listdir:
        writer = pd.ExcelWriter(os.path.join(filepath_home, current_datetime_evening))
        for sheet_name in salary_sheets.keys():
            salary_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
        writer.save()
    else:
        if current_datetime_morning not in listdir:
            writer = pd.ExcelWriter(os.path.join(filepath_home, current_datetime_morning))
            for sheet_name in salary_sheets.keys():
                salary_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
            writer.save()


sqldf = df[['****']]


sqldf.columns = ['****']

print(sqldf)

print(sqldf.loc[int(0)]['****'])

#sqldf.set_index('Date', inplace=True)

if __name__ == '__main__':


    ms = mssql()
    ldap = ms.im_execute('****')


def insert_into_oracle_raw(name, dfck, chunksize=1000):
    data = [tuple(x) for x in dfck.values]
    sqlcolumns = ', '.join([f'?' for x in range(1, len(dfck.columns) +1)])
    try:
        engine = sqlalchemy.create_engine(
            'mssql+pyodbc://opac:12375@10.40.189.10:1433/deptlog?driver=ODBC Driver 17 for SQL Server')
        connection = engine.raw_connection()

        for t in range(0, len(data), chunksize):

            cursor = connection.cursor()
            cursor.executemany(f"INSERT INTO {name} VALUES ({sqlcolumns})", data[t:t + chunksize])
            cursor.close()
            connection.commit()

        connection.close()

        return True, dfck
    except Exception as ex:
        print(ex)
        return False, ex
nametable = 'deptlog.dbo.erc_omod_for_komission_and_change'


sqldf.RequestId = sqldf.RequestId.fillna('None')
sqldf.ReqCnt = sqldf.ReqCnt.fillna(0)
sqldf.Sn = sqldf.Sn.fillna(-1)
sqldf.Sn = sqldf.Sn.apply(lambda x: -1 if x == 'Пусто' or type(x) != int else x)

sqldf.EnmOrig = sqldf.EnmOrig.fillna('None')
sqldf.BtsOrig = sqldf.BtsOrig.fillna('None')
sqldf.FingerOrig = sqldf.FingerOrig.fillna('None')
sqldf.FingerClone = sqldf.FingerClone.fillna('None')
sqldf.RemedyJobNumber = sqldf.RemedyJobNumber.fillna('None')
sqldf.Comments = sqldf.Comments.fillna('None')
sqldf.Filename = sqldf.Filename.fillna('None')
sqldf.Url = sqldf.Url.fillna('None')
sqldf.SyntDuType = sqldf.SyntDuType.fillna('None')
sqldf.OmodComments = sqldf.OmodComments.fillna('None')

#sqldf = sqldf.tail(10)


x = insert_into_oracle_raw(nametable, sqldf,1)
print(x)




