# -*- coding: utf-8 -*-
import sys, os
import json
import datetime, time
import pandas as pd
import numpy as np
import uuid
import traceback
import sendmail
from time import sleep
from enmlib import sql
from enmlib import bts
from enmlib import lic as enmlic
from enmlib import logtofile as l2f
from enmlib import lic_calculator as lc
from enmlib import main_proc as mp
from enmlib import enm
from enmlib import bts_ssh as bssh

from multiprocessing import Process, Pool
import multiprocessing
import multiprocessing.pool

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', -1)

class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)


class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

enm1 = {'path': '****', '****': '****', '****': '****'}
enm2 = {'path': '****', '****': '****', '****': '****'}
enm3 = {'path': '****', '****': '****', '****': '****'}
enm4 = {'path': '****', '****': '****', '****': '****'}
enm5 = {'path': '****', '****': '****', '****': '****'}
enm6 = {'path': '****', '****': '****', '****': '****'}
enm7 = {'path': '****', '****': '****', '****': '****'}
enm8 = {'path': '****', '****': '****', '****': '****'}
enm9 = {'path': '****', '****': '****', '****': '****'}
enm10 = {'path': '****', '****': '****', '****': '****'}


enm_all = {
    '****':  ****,
}

constpath = '/mnt/win/LicenseStorage/Ericsson/'

table_list = []
hwresult_list = []

args = sys.argv[1:]
filename = None
fullconf = False
upload = False

try:
    if len(args) > 0:
        if '--upload' in args:
            upload = True
    
        if '--full' in args:
            fullconf = True
            
        if '--filename' in args:
            # запускаем из файла
            filename = args[args.index('--filename') +1]
            eu = pd.read_csv(f'/home/****/{filename}', sep=';', engine='python')
            print(f'load from file-> {filename}')
            
        else:
            # запускаем из списка плохишей
            eu = sql.get_df_from_sql('****', '', '****')
            eu = eu[['****']].drop_duplicates()
            eu = eu.rename(columns={"****": "****"}).reset_index()
            eu = eu[['****']]
            print('load from sql -> ****')
        
    else:
        print('Запуск невозможен, нет агрументов!!')
        exit()

    # Последнее предупреждение
    print('fullconf -> ' + str(fullconf))
    print('upload   -> ' + str(upload))
    
    print('Press "Enter" to continue or "Ctrl+C" to abort')
    input()

    # Если колонки нет, то добавляем дефолтные значения
    if 'FINGERPRINT_OLD' not in eu.columns:
        eu['FINGERPRINT_OLD'] = None
        
    if 'FINGERPRINT_NEW' not in eu.columns:
        eu['FINGERPRINT_NEW'] = None
        
    if '****' not in eu.columns:
        eu['****'] = 0
        
    if '****' not in eu.columns:
        eu['****'] = 0
        
except Exception as ex:
    print(ex)

# список исключений
try:
    exclude_list = sql.get_df_from_sql('****', '', '****')
except Exception as ex:
    print(ex)

def isexclude(btsname):
    if btsname in exclude_list.bts.to_list():
        return True
    return False

startdt = datetime.datetime.now()
starttime = time.time()

# Очистка списка несинхронизированных БС в таблице t_apple_enm_orders
r, e = sql.clear_unsync()
l2f.log2f('clear.log', 'Result: ' + str(r))
l2f.log2f('clear.log', 'Errors: ' + str(e))

#  Опрос БС с аварией License Key File Fault  --- Нужно проверять наличие незакрытой заявки!!!
l2f.log2f('work.log', 50 * '*')
l2f.log2f('work.log', '****')
print('****')
l2f.log2f('work.log', 50 * '*')
cli_filter = '****'

def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

def fn_check_license_key_fault(enm_addr, cli_filter):
    alm = pd.DataFrame()
    cnt = 0
    try:
        print(f'LKF fault from ' + enm_addr['path'])
        alm, cnt = enm.get_active_alarm(enm_addr, '*', cli_filter)
    except Exception as ex:
        print(ex)
    try:
        print(alm.to_string())
        l2f.log2f('keyfault.log', 50 * '*')
        l2f.log2f('keyfault.log', enm_addr)
        l2f.log2f('keyfault.log', alm.to_string())
        l2f.log2f('work.log', alm.to_string())

    except Exception as ex:
        print(ex)
    if cnt > 0:
        NodeNameList = alm.NodeName.to_list()
        NodeNameList = list(set(NodeNameList))

        for i in range(len(NodeNameList)):
            try:
                # Проверяем есть ли БС в нашем листе исключений
                if isexclude(NodeNameList[i]):
                    l2f.log2f('work.log', NodeNameList[i] + ' is SKIPPED! BTS in Exception List')
                    continue

                try:
                    print('[', i, 'from', len(NodeNameList), ']', NodeNameList[i])
                except Exception as ex:
                    print(ex)
                try:
                    fulllic, shortlic, currentenm, omcname, ne, hwresult = None, None, None, None, None, None
                    fulllic, shortlic, currentenm, omcname, ne, hwresult = enmlic.inbts(NodeNameList[i])
                except Exception as ex:
                    print(ex)
                l2f.log2f('work.log', str(fulllic))
                l2f.log2f('work.log', str(shortlic))
                succ = fulllic['success']
                msg = fulllic['message']
                print('isFound:', ne.isfound, 'isSync:', ne.issync, 'neType:', ne.neType)
                if ne.isfound and ne.issync:
                    try:
                        euleft, eustate = ne.getEU()
                    except Exception as ex:
                        print(ex)
                    try:
                        iuleft, iustate = ne.getIU()
                    except Exception as ex:
                        print(ex)

                    print('BTS:', ne.name, 'eustate:', eustate, 'euleft:', euleft, 'iustate:', iustate, 'iuleft:', iuleft)
                    try:
                        ordlist, newid = sql.insert_to_orders_list(NodeNameList[i], json.dumps(shortlic, default=str))
                        #было shortlic
                        if ordlist:
                            l2f.log2f('reorder.log', newid)
                            l2f.log2f('reorder.log', str(shortlic))
                            l2f.log2f('reorder.log', str(fulllic))
                    except Exception as ex:
                        print(ex)

                    print('Inserted to ENM Order list:', ordlist)
                    l2f.log2f('work.log', 'Inserted to ENM Order list for ' + NodeNameList[i]
                              + ' is ' + str(ordlist))

            except Exception as mex:
                print(mex)

#fn_check_license_key_fault(enm1, cli_filter)
#fn_check_license_key_fault(enm2, cli_filter)
#fn_check_license_key_fault(enm3, cli_filter)
#fn_check_license_key_fault(enm4, cli_filter)
#fn_check_license_key_fault(enm5, cli_filter)
#fn_check_license_key_fault(enm6, cli_filter)
#fn_check_license_key_fault(enm7, cli_filter)
#fn_check_license_key_fault(enm8, cli_filter)
#fn_check_license_key_fault(enm9, cli_filter)
#fn_check_license_key_fault(enm10, cli_filter)


def UseLessMethodLKFFault(enm):
    info(enm_all[enm])
    try:
        fn_check_license_key_fault(enm_all[enm], cli_filter)
        #return elm_lkff
    except Exception as exc:
        print(exc)
        #return None

def fn_check_eu_iu(enm_addr):
    try:
        print('get EU', enm_addr['path'])
        eu = mp.getunlock(enm_addr).drop_duplicates(subset='NodeId', keep='last')
        return eu
    except Exception as e:
        eu = pd.DataFrame()
        l2f.log2f(os.path.join('main', 'euiu.log'), 'ERROR ' + enm_addr['path'] + ' ' + str(e))
        track = traceback.format_exc()
        l2f.log2f(os.path.join('main', 'euiu.log'), 'ERROR ' + enm_addr['path'] + ' ' + str(track))
        return eu


#  Проверка на ****
l2f.log2f(os.path.join('main', 'euiu.log'), 50 * '*')
l2f.log2f(os.path.join('main', 'euiu.log'), 'Check  ****')
print('Check  EU/IU')
l2f.log2f(os.path.join('main', 'euiu.log'), 50 * '*')

'''
eu1 = fn_check_eu_iu(enm1)
eu2 = fn_check_eu_iu(enm2)
eu3 = fn_check_eu_iu(enm3)
eu4 = fn_check_eu_iu(enm4)
eu5 = fn_check_eu_iu(enm5)
eu6 = fn_check_eu_iu(enm6)
eu7 = fn_check_eu_iu(enm7)
eu8 = fn_check_eu_iu(enm8)
eu9 = fn_check_eu_iu(enm9)
eu10 = fn_check_eu_iu(enm10)
'''



def UseLessMethodEU(enm):
    info(enm_all[enm])
    try:
        elm_eu = fn_check_eu_iu(enm_all[enm])
        return elm_eu
    except Exception as exc:
        print(exc)
        return None

def CheckLicense(eu):
#for i in range(len(eu)):
    worklog = os.path.join('main', 'euiu', f'{eu["NodeId"]}.log')
    if isexclude(eu['NodeId']) and False:  # никогда не выполнится
        l2f.log2f(worklog, eu['NodeId'] + ' is SKIPPED! BTS in Exception List')
        l2f.log2f('CSV_LIST_CHECK.log', eu['NodeId'] + ' is SKIPPED! BTS in Exception List')
        return None, None

    try:
        l2f.log2f(worklog, str(uuid.uuid4()))
        ne = bts.Bts(eu['NodeId'])

        # проверяем на синхронизацию и синхрим если нужно
        if not ne.issync:
            ne.SyncBTS()
            ne = bts.Bts(eu['NodeId'])

        if not ne.issync and False:
            l2f.log2f('CSV_LIST_CHECK.log', eu['NodeId'] + ' is SKIPPED! BTS NOT SYNC')
            print(bcolors.OKCYAN, 'BTS NOT SYNC', bcolors.ENDC)

            sendmail.send_email(
                f'Базовая станция {ne.name} не синхронизирована, лицензия не установлена',
                'Отправьте станцию на пересинхронизацию',
                ['vakisil@mts.ru', 'dipupyni@mts.ru', 'vakobzev@mts.ru'], ['ocran@mts.ru'], '')

            return None, None
        
        # апдейтим пустые 
        if eu['FINGERPRINT_OLD'] == np.NaN or eu['FINGERPRINT_OLD'] is None or str(eu['FINGERPRINT_OLD']) == '' or str(eu['FINGERPRINT_OLD']).lower() == 'nan':
            eu['FINGERPRINT_OLD'] = ne.finger
        
        # запускаем расчет 
        fulllic, shortlic, currentenm, omcname, ne, hwresult = None, None, None, None, None, None
        fulllic, shortlic, currentenm, omcname, ne, hwresult = enmlic.inbts(
            eu['NodeId'], fullconf, eu['FINGERPRINT_OLD']
        )

        l2f.log2f('CSV_LIST_CHECK.log', eu['NodeId'], 'Полная лицензия \n' + str(fulllic))
        l2f.log2f('CSV_LIST_CHECK.log', eu['NodeId'],
                  'Короткая лицензия с учетом установленной \n' + str(shortlic))
        
        def proc(lic, eu):
            # заменяем фингерпринт в заявке
            if 'data' in lic and eu['FINGERPRINT_OLD'] is not None:
                if len(lic['data']) > 0:
                    if eu['FINGERPRINT_OLD'] is not None:
                        lic['data']['fingerprint'] = eu['FINGERPRINT_OLD']
            
            # апдейтим данные полосы и мощности
            **** = lic['data'].get('****', 0) + eu['****']
            if **** > 0:
                lic['data']['****'] = ****
                
            **** = lic['data'].get('****', 0) + eu['****']
            if **** > 0:
                lic['data']['****'] = ****
            
            return lic
            
        # если лицуха успешно посчиталась, то добавляем имя БС в hwresult
        if shortlic['success'] == True or fulllic['success'] == True:
            hwresult['NODEID'] = eu['NodeId']

        fulllic = proc(fulllic, eu)
        shortlic = proc(shortlic, eu)

        if upload:
            # отправляем в БД
            ordlist, newid = sql.insert_to_orders_list(
                eu['NodeId'], 
                json.dumps(fulllic if fullconf else shortlic, default=str)
            )
        
        # возвращаем результат
        if fullconf:
            return fulllic['data'], hwresult 
        else:
            return shortlic['data'], hwresult
            
    except Exception as e:
        print(bcolors.OKCYAN, 'EXCEPTION', bcolors.ENDC)
        print(e)


def UseLessMethodLicense(order):
    info(order)
    try:
        return CheckLicense(order)
        pass
    except Exception as e:
        print(order['NodeId'])
        print(e)
        pass

m_df = None
rsr_df = None

if __name__ == '__main__':

    try:
        # наш ежедневный репорт 
        m_df = sql.get_df_from_sql('****', '', '****')
        
        # план РСР
        rsr_df = sql.get_df_from_sql('****', '', '****')
    except Exception as ex:
        print(f'!!! exception -> {ex}')
        exit()

    orders = eu.to_dict('records')

    # запускаем опрос в несколько процессов
    with MyPool(12) as p:
        result = p.map(UseLessMethodLicense, orders)

    try:
        # собираем исходные данные для датафреймов
        for x in result:
            if x is not None:
                if x[0] is None:
                    pass
                else:
                    table_list.append(x[0])
                    hwresult_list.append(x[1])

        tldf = pd.DataFrame(table_list)
        l2f.log2f('****', tldf)
        l2f.log2f('****', tldf.to_csv())

        # затыкаем дыры в сережином расчете
        if '****' in tldf:
            tldf.**** = tldf.apply(lambda x: np.NaN if x.**** == 2.0 and x.dutype == 'DUS' else x.****, axis=1)
        if '****' in tldf:
            tldf.**** = tldf.apply(lambda x: np.NaN if x.**** == 2.0 and x.dutype == 'DUW' else x.****, axis=1)

        tl2 = []

        # список постоянных колонок, которые должны идти первыми
        tldf_columns = [
            'BTSName', 'fingerprint', 'release', 'dutype', 'lmstate', 'sn','current','model'
        ]

        for column in tldf.columns:
            try:
                if column in [
                    'BTSName', 'fingerprint', 'release', 'dutype', 'lmstate', 'sn','current','model',
                    'OLD_****', 'OLD_****',
                    '****', '****', '****', '****'
                ]:
                    pass
                else:
                    tl2.append(column)
                    
            except Exception as e:
                print(e)
                pass
        
        # определяем EU Only
        test = tldf[tl2].isna()
        tldf2 = tldf[tldf.index.isin(test[test].dropna().index.to_list())]

        for i, c in enumerate(tldf_columns):
            tldf.insert(i, c, tldf.pop(c))
            tldf2.insert(i, c, tldf2.pop(c))

        l2f.log2f('CSV_LIST_CHECK_FINAL_EU_1.log', tldf2)
        l2f.log2f('CSV_LIST_CHECK_FINAL_EU.log', tldf2.to_csv())

        hwresult_df = pd.DataFrame(hwresult_list)
        l2f.log2f('CSV_LIST_CHECK_CONF.log', hwresult_df.to_csv())

        # записываем конфигурацию в таблицу
        b = sql.truncate('t_erc_eu_config', 'db_opac.dbo')
        b = sql.insert_to_table(hwresult_df.astype('str'), 't_erc_eu_config', dbschema='db_opac.dbo')

        # и в Excel файл
        with pd.ExcelWriter(os.path.join(l2f.currenlogpath, 'CONF.xlsx')) as writer:
            hwresult_df.to_excel(writer, sheet_name='CONF')

        # еще один файл c данными EU/IU отправляется по почте 
        with pd.ExcelWriter(os.path.join(l2f.currenlogpath, 'CSV_LIST_CHECK_FINAL.xlsx')) as writer:
            tldf.to_excel(writer, sheet_name='CSV_LIST_CHECK_FINAL')
            tldf2.to_excel(writer, sheet_name='CSV_LIST_CHECK_FINAL_EU')
            hwresult_df.to_excel(writer, sheet_name='CONF')

        emails = ["****"]
        cc_emails = ["****", '****']
        bcc_emails = [""]
        subject = "****"
        body_text = "****"

        sendmail.send_email_with_attachment(subject, body_text, emails, cc_emails, bcc_emails,
                                        os.path.join(l2f.currenlogpath, '****'))


        # тоже самое но с планом РСР
        m_df_with_result = pd.merge(m_df, hwresult_df, on='****', how='****')
        m_df_with_result_with_rsr = pd.merge(m_df_with_result, rsr_df, right_on='****', left_on='NODEID', how='left')

        print(m_df_with_result)

        with pd.ExcelWriter(os.path.join(l2f.currenlogpath, '****')) as writer:
            tldf.to_excel(writer, sheet_name='****')
            tldf2.to_excel(writer, sheet_name='****')
            hwresult_df.to_excel(writer, sheet_name='****')
            m_df_with_result.to_excel(writer, sheet_name='****')
            m_df_with_result_with_rsr.to_excel(writer, sheet_name='****')

        sendmail.send_email_with_attachment(subject, body_text, emails, cc_emails, bcc_emails,
                                        os.path.join(l2f.currenlogpath, '****'))

    except Exception as e:
        def get_traceback(ex):
            lines = traceback.format_exception(type(ex), ex, ex.__traceback__)
            return '\r\n'.join(lines)

        emails = ["****", "****", "****", "****"]
        cc_emails = ["****", '****']
        bcc_emails = [""]

        subject = "Cброс EU/IU"
        body_text = "EXCEPTION " + str(e) + '/r/n' + get_traceback(e)

        sendmail.send_email(subject, body_text, emails, cc_emails, bcc_emails)
