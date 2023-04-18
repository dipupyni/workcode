#_*_coding: utf-8_*_
import pandas as pd
import os
from msconnect import mssql
from datetime import datetime
import smtplib


import email
import email.mime.application
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', -1)


if __name__ == '__main__':

    ms = mssql()
    #ldap0 = ms.im_execute("TRUNCATE table dbo.erc_license_clones_py_ver insert into dbo.erc_license_clones_py_ver select ENM_ORIG, BTS_ORIG, FINGER_ORIG, ENM_CLONE, BTS_CLONE, FINGER_CLONE, DATE, FILENAME, URL, EU_STATUS, EU_LEFT, EU_EXPIRE_DATE, WeekNum, cast(ISNULL(RG,0) as int) as RG, count(BTS_ORIG) over (partition by WeekNum, RG) as CountBtsNum from (select ENM_ORIG, BTS_ORIG, FINGER_ORIG, ENM_CLONE, BTS_CLONE, FINGER_CLONE,DATE,FILENAME,URL, EUACTIVATIONSTATE AS EU_STATUS, EUACTIVATIONLEFT AS EU_LEFT, EUEXPIRATION AS EU_EXPIRE_DATE, concat('w',(DATEDIFF(day, '2022-01-03 00:00:00', DATE)/7+1)) as WeekNum, case when substring (BTS_ORIG, 1, case when charindex('_', BTS_ORIG, 1) = 0 then len(BTS_ORIG) else charindex('_', BTS_ORIG, 1) - 1 end) = 'BTS' then substring (BTS_ORIG, 5, case when charindex('_', BTS_ORIG, 4) = 0 then len(BTS_ORIG) else charindex('_', BTS_ORIG, 4) - 2 end) else substring (BTS_ORIG, 1, case when charindex('_', BTS_ORIG, 1) = 0 then len(BTS_ORIG) else charindex('_', BTS_ORIG, 1) - 1 end) end RG  	from dbo.erc_license_clones as t1 left join [db_opac].[dbo].[t_erc_eu_status_new] as t2 ON t1.BTS_ORIG=t2.NODEID WHERE COMMENTS <> 'Донор установлен' and BTS_ORIG not like '%TEST%') t3")
    ldap = ms.fetch('select * from ****')
    dfLdap = pd.DataFrame(ldap)
    ldap2 = ms.fetch('select * from ****')
    df2Ldap = pd.DataFrame(ldap2)

dfLdap.columns = ['****']
rg_list = list(set(list(dfLdap['****'])))
dfLdap.set_index('****', inplace=True)
df2Ldap.columns = ['****']
df2Ldap.set_index('****', inplace=True)

filemainpath = '****'
current_datetime = datetime.now().date()
print(rg_list)
#print(df2Ldap.loc[23]['Имя'])

#dfLdap.loc['84'].to_excel(os.path.join(filemainpath, 'Список доноров 89 регион {}.xls'.format(current_datetime)))

for df in rg_list:
    dfLdap.loc[df].to_excel(os.path.join(filemainpath, '****'.format(df, current_datetime)))

for df in rg_list:
    #dfLdap.loc[df].to_excel(os.path.join(filemainpath, 'Список доноров {} регион {}.xls'.format(df, current_datetime)))
    # Create a text/plain message
    msg = MIMEMultipart()
    msg['****'] = '****'
    msg['****'] = '****'
    msg['****'] = df2Ldap.loc[int(df)]['****']
    msg["****"] = '****'

    # The main body is just another attachment
    body = MIMEText("""
                    ****
                    """)
    msg.attach(body)

    # PDF attachment
    filenametrue = '****'.format(df, current_datetime)
    filename = os.path.join(filemainpath, '****'.format(df, current_datetime))
    fp = open(filename, 'rb')
    att = email.mime.application.MIMEApplication(fp.read(), _subtype="xls")
    fp.close()
    att.add_header('****', 'attachment', filename=filenametrue)
    msg.attach(att)
    s = smtplib.SMTP('****', 25)
    s.starttls()
    s.login('****', 'CF3IX1AR')
    s.sendmail(msg['From'], msg["To"].split(",") + msg["Cc"].split(","), msg.as_string())
    s.quit()
































