# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 09:46:09 2025

@author: Agropilot-Project
"""

import streamlit as st
import pandas as pd
import psycopg2
import httpx
import datetime
import time
from io import StringIO
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings('ignore')
#import altair as alt
#from sqlalchemy import create_engine


#Возвращает число месяцев между датами
@st.cache_data
def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


#Определяет тип объекта
@st.cache_data
def return_type (obj_name):
    import re
    #1
    passenger = ['geely', 'dokker', 'karoq', 'octavia', 'logan', 'toyota', 'mercedes', 'duster', 'renault', 'гранта', 'уаз', 'niva', \
                 'chevrolet', 'lada', 'нива', 'sandero', 'лада', 'дастер', 'accent', 'chery', 'tiggo', 'калина', \
                'патриот', 'ваз']
    #2
    cargo = ['зил', 'next', 'саз', 'камаз', 'ммз',  'газ']
    #3
    bus = ['travel', 'transit']
    #4
    tractors = ['atles', '8335r', '770', '8430', '8rx', '9420', 'мтз', '82.1', 'кировец', 'yto', 'jcb', 'manitou', 'рсм', '9110r', \
               '9410', 'T8040', 'dieci', 'gehl', '8310r']
    #5
    sprayers = ['туман', 'барс']
    #6
    harvesters = ['tucano', 's760', '760', 'acros', 'акрос', 'торум', 'комбайн', 'cs6090', 'across', 'torum']
    #7
    special = ['scorpion', 'атз', 'jic','бульдозер', 'кран']
    #8
    equipment = ['сеялка']
    res = 0
    obj_name = str.lower(obj_name)
    new_s = re.sub(r"[_()-]", " ", obj_name)
    if set(passenger) & set(new_s.split()):
        res = 1
    elif set(cargo) & set(new_s.split()):
        res = 2
    elif set(bus) & set(new_s.split()):
        res = 3
    elif set(tractors) & set(new_s.split()):
        res = 4
    elif set(sprayers) & set(new_s.split()):
        res = 5
    elif set(harvesters) & set(new_s.split()):
        res = 6
    elif set(special) & set(new_s.split()):
        res = 7
    elif set(equipment) & set(new_s.split()):
        res = 8
    else: res = 9
    return res

@st.cache_data
def parce_fort_json2(data):
    # Создание пустого списка для хранения преобразованных данных
    flattened_data = []
    
    # Проход по каждому объекту в данных
    for item in data:
        oid = item['oid']
        obj_name = item['obj_name']
        
        # Проверяем, есть ли периоды у объекта
        if 'periods' in item and item['periods']:  # Если periods существует и не пустой
            # Проход по каждому периоду в объекте
            for period in item['periods']:
                # Проверяем, есть ли параметры в периоде
                if 'prms' in period and period['prms']:  # Если prms существует и не пустой
                    # Создаем словарь для хранения данных текущего периода
                    period_data = {
                        'oid': oid,
                        'obj_name': obj_name,
                        'begin': period['begin'],
                        'end': period['end'],
                        'isTotal': period['isTotal'],
                        'name': period.get('name', '')  # Используем get для избежания ошибок, если ключа нет
                    }
                    
                    # Флаг для проверки, есть ли хотя бы один параметр больше нуля
                    has_positive_value = False
                    
                    # Проход по каждому параметру в периоде
                    for param in period['prms']:
                        # Добавляем каждый параметр как отдельный столбец
                        param_name = param['name']
                        param_value = param['value']
                        
                        # Преобразуем значение в число, если это возможно
                        try:
                            param_value_num = float(param_value)
                            if param_value_num > 0:  # Проверяем, больше ли значение нуля
                                has_positive_value = True
                        except (ValueError, TypeError):
                            # Если значение не может быть преобразовано в число, пропускаем
                            pass
                        
                        period_data[param_name] = param_value
                    
                    # Добавляем период в таблицу, если хотя бы один параметр больше нуля
                    if has_positive_value:
                        flattened_data.append(period_data)
    
    # Преобразование списка в DataFrame
    df = pd.DataFrame(flattened_data)
    return df


# Считываем по API список компаний из Форта
@st.cache_data
def loadcompanylist():
    url = 'https://glonassagro.com/'
    username = 'rvl_testapi'
    password = 'rvltestapi2024'
    API = 'api/integration/v1/'
    method = 'connect'
    headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
    data = {'login' : username,
            'password' : password,
            'lang' : 'ru-ru',
            'timezone' : '0'}
    method = 'connect'
    headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
    client = httpx.Client()
    response = client.get(url+API+method, timeout=1000, headers=headers, params=data)
    sessionid = response.headers['SessionId']
    headers = {'SessionId':sessionid}
    path = 'getcompanieslist'
    resp = client.get(url+API+path, headers=headers)
    df = pd.DataFrame.from_dict(resp.json()['companies'])
    method = 'disconnect'
    response = client.get(url+API+method, timeout=1000)
    return df

#Запрашиваем список объектов компании
@st.cache_data
def loadobjectlist(id_company):
    url = 'https://glonassagro.com/'
    username = 'rvl_testapi'
    password = 'rvltestapi2024'
    API = 'api/integration/v1/'
    method = 'connect'
    headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
    data = {'login' : username,
            'password' : password,
            'lang' : 'ru-ru',
            'timezone' : '0'}
    method = 'connect'
    headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
    client = httpx.Client()
    response = client.get(url+API+method, timeout=1000, headers=headers, params=data)
    sessionid = response.headers['SessionId']
    headers = {'SessionId':sessionid}
    path = 'getobjectslist'
    query = {'companyId': id_company}
    resp = client.get(url+API+path, headers=headers, params=query)
    df = pd.DataFrame.from_dict(resp.json()['objects'])
    method = 'disconnect'
    response = client.get(url+API+method, timeout=1000)
    return df

#Запрашиваем статистику по объектам за выбранный период
@st.cache_data
def loadobjectsstst(objects_list, start_date, stop_date):
    url = 'https://glonassagro.com/'
    username = 'rvl_testapi'
    password = 'rvltestapi2024'
    API = 'api/integration/v1/'
    method = 'connect'
    headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
    data = {'login' : username,
            'password' : password,
            'lang' : 'ru-ru',
            'timezone' : '0'}
    objects =";".join(str(element) for element in objects_list)
    method = 'connect'
    headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
    params = 'dist;run_time;stop_time;idle_time;max_speed;avg_speed;motohours;start_move_time;stop_move_time;all_fuel;run_fuel;\
            idle_fuel;start_fuel_level;stop_fuel_level;fuelings;drains'
    path = 'getobjectsreport'
    client = httpx.Client()
    response = client.get(url+API+method, timeout=1000, headers=headers, params=data)
    sessionid = response.headers['SessionId']
    headers = {'SessionId':sessionid,
          'Content-Type' : 'application/json'}
    query = {'date_from': start_date,
        'date_to': stop_date,
        'objuids': objects,
        'split':'day',
        'param': params}
    resp = client.get(url+API+path, timeout=5000, headers=headers, params=query)
    obj_dict = resp.json()
    df = parce_fort_json2(obj_dict)
    method = 'disconnect'
    response = client.get(url+API+method, timeout=1000)
    return df
    

#Заправшиваем перечень систем
@st.cache_data
def load_table_system():
    with psycopg2.connect(dbname="postgres",
                    user="postgres",
                    password="AgroPilot2025",
                    host="82.142.178.174",
                    port="5432") as conn:
        sql = "SELECT * FROM telesystems;"
        dat = pd.read_sql_query(sql, conn)
    return dat
#Проверка на наличие компании в базе по имени
@st.cache_data
def company_exists(table_str):
    conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="AgroPilot2025",
                host="82.142.178.174",
                port="5432"
                )
    exists = 0
    try:
        cur = conn.cursor()
        cur.execute("select id_company from company where short_name= '" + table_str+"';")
        exists = cur.fetchone()
        cur.close()
        conn.close()
        if exists != None:
            exists = exists[0]
        else:
            exists = 0
    except psycopg2.Error as e:
        print (e)
    return exists

#Добавление компании в базу
@st.cache_data
def insert_company(short_name, long_name, address, id_system, id_insys):
    conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="AgroPilot2025",
                host="82.142.178.174",
                port="5432"
                )
    exists = False
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO company (short_name, full_name, address, sys_id, id_insys) VALUES (%s, %s, %s, %s);", \
                    (short_name, long_name, address, id_system))
        conn.commit()
        exists = True
    except psycopg2.Error as e:
        print (e)
        conn.rollback()
    return exists
#Проверка на наличие объектов в БД
@st.cache_data
def objects_exists(id_company):
    conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="AgroPilot2025",
                host="82.142.178.174",
                port="5432"
                )
    exists = False
    id_company = str(id_company)
    try:
        cur = conn.cursor()
        cur.execute("select exists(select * from objects where id_company=" + id_company + ");")
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return exists

#Вставка объектов в таблицу БД
@st.cache_data
def objects_insert(df):
# Устанавливаем подключение к базе данных PostgreSQL
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password="AgroPilot2025",
        host="82.142.178.174",
        port='5432'
    )    
    flag = False
    sio = StringIO()
    df.to_csv(sio, index=None, header=None)
    sio.seek(0)
    try:
        with conn.cursor() as c:
            c.copy_expert(
                sql="""
                COPY objects (
                    id_company, 
                    id_sys, 
                    object_name, 
                    id_model, 
                    id_type, 
                    reg_number, 
                    imei,
                    status, 
                    last_date,
                    oid
                    ) FROM STDIN WITH CSV""",
                file=sio
            )
        conn.commit()
        flag = True
    except psycopg2.Error as e:
        print (e)
        conn.rollback()
        flag = False
    return flag
#Возвращает ID объектов в БД
@st.cache_data
def objects_return(id_company):
    conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="AgroPilot2025",
                host="82.142.178.174",
                port="5432"
                )
    id_company = str(id_company)
    try:
        cur = conn.cursor()
        sql = "select * from objects where id_company= " + id_company+";"
        dat = pd.read_sql_query(sql, conn)
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return dat

#Вставка параметров объектов в БД
@st.cache_data
def objects_param_insert(df):
# Устанавливаем подключение к базе данных PostgreSQL
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password="AgroPilot2025",
        host="82.142.178.174",
        port='5432'
    )    
    flag = False
    sio = StringIO()
    df.to_csv(sio, index=None, header=None)
    sio.seek(0)
    try:
        with conn.cursor() as c:
            c.copy_expert(
                sql="""
                COPY object_statistic (
                    id_object, 
                    period_begin, 
                    period_end, 
                    dist, 
                    run_time, 
                    stop_time, 
                    idle_time,
                    max_speed, 
                    avg_speed,
                    motohours,
                    start_move_time,
                    stop_move_time,
                    all_fuel,
                    run_fuel,
                    idle_fuel,
                    start_fuel_level,
                    stop_fuel_level,
                    fuelings,
                    drains,
                    id_driver
                    ) FROM STDIN WITH CSV""",
                file=sio
            )
        conn.commit()
        flag = True
    except psycopg2.Error as e:
        print (e)
        conn.rollback()
        flag = False
    return flag    
    
#Создаем боковое меню
st.set_page_config(page_title="Импорт объектов", page_icon="📈")
st.markdown("# Импорт объектов и базовых параметров")
st.sidebar.header("Импорт данных")


dictsys = load_table_system()
syslist = st.radio(
    "Выберите систему (данные подружены из хранилища АГРОштурман):",
    dictsys['system_name'],
    index=None,
)
st.write("Вы выбрали ", syslist)
if syslist != None:
    sysnum = dictsys.loc[dictsys['system_name'] == syslist, 'id_system'].item()
    st.write("Номер системы: ", sysnum)
#Если выбран Форт
if syslist == 'Форт Монитор':
#считываем и выводим список компаний
    df_company = loadcompanylist()
    CompanyList = st.selectbox(
       'Выберите компанию (данные загружены по API из системы Форт Монитор):',
        df_company['name'].unique())
    comp_id = company_exists(CompanyList)
    if comp_id != 0:
        'Данная компания уже заведена в базу данных'
        st.write("Номер компании в базе: ", comp_id)
        id_company = df_company.loc[df_company['name'] == CompanyList]['id'].item()
    else: 
        'Вы выбрали: ', CompanyList
        id_company = df_company.loc[df_company['name'] == CompanyList]['id'].item()
        st.write("ID in Fort: ", id_company)
 #Предлагаем заполнить название и адрес
        longname = st.checkbox('Добавить полное наимнование компании (рекомендуется)')
        companyaddress = st.checkbox('Добавить адрес компании (рекомендуется)')
        if longname:
            fullname = st.text_input('Введите полное наименование компании (будет записано в БД)')
        if companyaddress:
            adrcompany = st.text_input('Введите адрес компании (понадобится в дальнейшем для отображения на карте)')
#Предлагаем выбрать дату скачивания  
    d = st.date_input("Введите дату с которой начнется импорт данных:", value=None)
    now = datetime.datetime.now().date()
    if d != None:
        st.write("Импорт данных в систему аналитики будет произведен начиная с ", d, " по ", now)
        if st.sidebar.button("Импорт данных", type="primary",   disabled=False):
            status_text = st.sidebar.empty()
            progress_bar = st.sidebar.progress(0)
#Вносим компанию в БД если ее еще там нет
            if comp_id == 0:
                if insert_company(CompanyList, fullname, adrcompany, sysnum, id_company):
                    progress_bar.progress(5)
                    status_text.text("Записываем в базу параметры компании")
                    time.sleep(0.05)
                else:
                    status_text.text("Ошибка записи")
                    time.sleep(0.05)
            progress_bar.progress(5)
            comp_id = company_exists(CompanyList)
            progress_bar.progress(10)
            status_text.text("Запись прошла успешно, ID компании: %comp_id%%" % comp_id)
            time.sleep(0.05)
            progress_bar.progress(15)
            status_text.text("Формируем запрос к " + syslist)
            time.sleep(0.05)
#Проверяем наличие объектов в БД 
            status_text.text("Проверяем наличие объектов в базе")
            progress_bar.progress(20)
            time.sleep(0.05)
            if objects_exists(comp_id):
                progress_bar.progress(100)
                status_text.text("Объекты уже есть в базе. Импорт данных прерван.")
                time.sleep(0.05)
            else:
                progress_bar.progress(25)
                status_text.text("Получаем список объектов из системы " + syslist)
                time.sleep(0.05)
#Тут делаем выборку объектов их обработку, проверку на наличие в БД и запись в БД            
                df_objects = loadobjectlist(id_company)
                status_text.text("Список объектов получен")
                time.sleep(0.05)
                progress_bar.progress(30)
                dict_o = df_objects['id'].to_string(index=False).split('\n')
#                objects =";".join(str(element) for element in dict)
                status_text.text("Проводим очистку данных")
                time.sleep(0.05)
                df_objects.rename(columns={'id':'oid', 'name':'object_name','IMEI':'imei', 
                                           'lastData':'last_date', 'direction':'reg_number'}, inplace=True)
                df_objects.drop(['groupId', 'icon', 'rotateIcon', 'iconHeight', 'iconWidth', 'lat', 'lon', 'move'], axis=1, inplace=True)            
    #Подставляем ID компании в БД
                df_objects['id_company'] = comp_id
                progress_bar.progress(35)
                time.sleep(0.05)
                df_objects['id_sys'] = sysnum
                df_objects['id_model'] = None
                progress_bar.progress(40)
                time.sleep(0.05)            
#Определяем типы техники по названию
                df_objects['id_type'] = df_objects.apply(lambda x: return_type(x['object_name']), axis=1)
                df_objects['reg_number'] = ''
                progress_bar.progress(45)
                time.sleep(0.05)
#Меняем порядок столбцов
                df_objects = df_objects.reindex(columns=['id_company', 'id_sys', 'object_name', 'id_model', 'id_type',
                                                         'reg_number', 'imei', 'status', 'last_date', 'oid'])
#Записываем объекты в БД
                status_text.text("Записываем объекты в БД")
                time.sleep(0.05)
                if objects_insert(df_objects):
                    progress_bar.progress(50)
                    time.sleep(0.05)
    #Получаем данные за выбранный период из Форта      
                    status_text.text("Читаем данные по объектам из " + syslist)
                    d_month = diff_month(now, d)
                    if d_month > 3:
                        df_stst = pd.DataFrame()
                        start_date = d
                        for i in range(1, d_month):
                            status_text.text("Считываем %i%% -й месяц" % i)
                            stop_date = d + relativedelta(months=i)
                            df_m = loadobjectsstst(dict_o, start_date, stop_date)
                            df_stst = pd.concat([df_stst, df_m], ignore_index=True)
                            start_date = stop_date + relativedelta(days=1)
                            progress_bar.progress(50+i)
                        start_date = stop_date + relativedelta(days=1)
                        stop_date = now
                        df_m = loadobjectsstst(dict_o, start_date, stop_date)
                        df_stst = pd.concat([df_stst, df_m], ignore_index=True)
                    else:
                        df_stst = loadobjectsstst(dict_o, d, now)
                    progress_bar.progress(60)
                    status_text.text("Данные считаны успешно")
                    time.sleep(0.05)
                    status_text.text("Производим очистку данных")
    #Обрабатываем информацию по параметрам объектов
                    drop_row_index = df_stst.loc[df_stst['isTotal']==True].index
                    progress_bar.progress(65)
                    time.sleep(0.05)
                    df_stst.drop(drop_row_index, inplace=True)
                    progress_bar.progress(70)
                    time.sleep(0.05)
                    df_stst['start_move_time'] = df_stst['start_move_time'].fillna(df_stst['begin'])
                    df_stst['stop_move_time'] = df_stst['stop_move_time'].fillna(df_stst['end'])
                    progress_bar.progress(75)
                    time.sleep(0.05)
                    df_stst[['start_move_time', 'stop_move_time']].fillna(df_stst[['begin', 'end']], inplace=True)
                    df_stst.fillna(0, inplace=True)
                    progress_bar.progress(80)
                    time.sleep(0.05)
                    df_stst['begin'] = pd.to_datetime(df_stst['begin'], format='%Y-%m-%d %H:%M:%S')
                    df_stst['end'] = pd.to_datetime(df_stst['end'], format='%Y-%m-%d %H:%M:%S')
                    df_stst['start_move_time'] = pd.to_datetime(df_stst['start_move_time'], format='%Y-%m-%d %H:%M:%S')
                    df_stst['stop_move_time'] = pd.to_datetime(df_stst['stop_move_time'], format='%Y-%m-%d %H:%M:%S')
                    progress_bar.progress(85)
                    time.sleep(0.05)
                    df_stst.rename(columns={'oid':'id_object', 'begin': 'period_begin', 'end': 'period_end', }, inplace=True)
#Считываем объекты из БД для подстановки ID
                    df1 = objects_return(comp_id)
                    df1 = df1.drop(columns=['id_company', 'id_sys', 'id_model', 'id_type', 'reg_number', 'imei', 
                               'status', 'last_date', 'oid'])
                    df2 = df1.merge(df_stst, left_on='object_name', right_on='obj_name', how='left')
                    df2.drop(['id_object', 'object_name', 'obj_name', 'isTotal', 'name'], axis=1, inplace=True)
                    df2['id_driver']=None
                    df2.rename(columns={'id_objects': 'id_object'}, inplace=True)
                    df2.drop(df2[df2['period_end'].isnull()].index, inplace=True)
                    progress_bar.progress(88)
                    time.sleep(0.05)
                    st.write(" ### Объекты")
                    df_objects
                    st.write(" ### Параметры")
                    df_stst
                    status_text.text("Записываем данные по объектам в БД")
                    time.sleep(0.05)
                    progress_bar.progress(93)
                    if objects_param_insert(df2):
                        progress_bar.progress(100)
                        status_text.text("Импорт данных завершен")
                        time.sleep(0.05)
                        st.write(" ### Импорт данных завершен")
                    else:
                        progress_bar.progress(100)
                        status_text.text("Сбой импорта данных")
                        time.sleep(0.05)
                        st.write(" ### Сбой импорта данных")
                else:
                    progress_bar.progress(100)
                    status_text.text("Сбой импорта данных")
                    time.sleep(0.05)
                    st.write(" ### Сбой импорта данных")