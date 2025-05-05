# -*- coding: utf-8 -*-
"""
Created on Fri Mar 28 12:45:58 2025

@author: Agropilot-Project
"""


import streamlit as st
import httpx
import pandas as pd
import psycopg2
import time
from io import StringIO
pd.options.display.max_columns = None

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

#Заправшиваем перечень компани
@st.cache_data
def load_table_company():
    with psycopg2.connect(dbname="postgres",
                    user="postgres",
                    password="AgroPilot2025",
                    host="82.142.178.174",
                    port="5432") as conn:
        sql = "SELECT * FROM company;"
        dat = pd.read_sql_query(sql, conn)
    return dat

#Запрашиваем список групп геозон
@st.cache_data
def loadgroupzonelist(id_company):
    url = 'https://glonassagro.com/'
    username = 'rvl_testapi'
    password = 'rvltestapi2024'
    API = 'api/integration/v1/'
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
    path = 'geozonegroups/list'
    query = {'companyId': 37}
    resp = client.get(url+API+path, headers=headers, params=query)
    df = pd.json_normalize(resp.json()['groups'])
    method = 'disconnect'
    response = client.get(url+API+method, timeout=1000)
    return df

#Запрашиваем список геозон
@st.cache_data
def loadzonelist(id_sys, id_company, comp_id):
    # Функция для вычисления среднего значения lt и ln
    def calculate_stats(points):
        lats = [point['lt'] for point in points]
        lons = [point['ln'] for point in points]
        return pd.Series({
            'min_lat': min(lats),
            'max_lat': max(lats),
            'min_lon': min(lons),
            'max_lon': max(lons),
            'mean_lat': sum(lats) / len(lats),
            'mean_lon': sum(lons) / len(lons)
        })
    url = 'https://glonassagro.com/'
    username = 'rvl_testapi'
    password = 'rvltestapi2024'
    API = 'api/integration/v1/'
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
    path = 'getgeozones'
    query = {'companyId': id_company}
    resp = client.get(url+API+path, headers=headers, params=query)
    data = resp.json()
    # Преобразование JSON в DataFrame
    df = pd.DataFrame(data)
    # Применение функции к столбцу points и создание новых столбцов
    df = df.join(df['points'].apply(calculate_stats))
# Удаление столбца points
    df = df.drop(columns=['type','points','radius', 'useForAddress', 'showOnMap', 'icon', 'sysIdent', 'color', 'descr'])
    df.rename(columns={'id': 'zone_sysid', 'groupId':'id_group', 'companyId': 'id_company', 'name':'zone_name', 
                        'dimension':'agro_area', 'agriName': 'address_zone'}, inplace=True)
    df['id_system'] = id_sys
    df['id_company'] = comp_id
    df['id_address'] = None
    df = df.reindex(columns=['id_company', 'id_system', 'id_group', 'zone_name', 'agro_area', 'id_address', 'min_lat', 'min_lon',
                             'max_lat', 'max_lon', 'address_zone', 'zone_sysid', 'mean_lat', 'mean_lon'])
    method = 'disconnect'
    response = client.get(url+API+method, timeout=1000)
    return df

#Проверка на наличие геозон в БД
@st.cache_data
def zone_exists(id_company):
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
        cur.execute("select exists(select * from agrozones where id_company=" + id_company + ");")
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return exists

#Загрузка групп геозон в БД
@st.cache_data
def group_insert(df):
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
                COPY group_agrozones (
                    id_company, 
                    group_name, 
                    id_address, 
                    min_lat, 
                    min_lon, 
                    max_lat, 
                    max_lon,
                    group_sysid, 
                    mean_lat,
                    mean_lon,
                    dimension
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

#Возвращает ID групп в БД
@st.cache_data
def group_return(id_company):
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
        sql = "select * from group_agrozones where id_company= " + id_company+";"
        dat = pd.read_sql_query(sql, conn)
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return dat

#Загрузка геозон в БД
@st.cache_data
def zone_insert(df):
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
                COPY agrozones (
                    id_company, 
                    id_system, 
                    id_group, 
                    zone_name, 
                    agro_area, 
                    id_address, 
                    min_lat,
                    min_lon, 
                    max_lat, 
                    max_lon, 
                    address_zone, 
                    zone_sysid, 
                    mean_lat, 
                    mean_lon
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
st.set_page_config(page_title="Импорт геозон", page_icon="🌾")
st.markdown("# Импорт геозон")
st.sidebar.header("Импорт геозон")
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
    df_company = load_table_company()
    CompanyList = st.selectbox(
       'Выберите компанию:',
        df_company['short_name'].unique())
    comp_id = df_company.loc[df_company['short_name'] == CompanyList, 'id_company'].item()
    comp_id_sys = df_company.loc[df_company['short_name'] == CompanyList, 'id_insys'].item()
    st.write("Вы выбрали ", CompanyList)
    st.write("ID в Форт ", comp_id_sys)
    st.write("ID в БД ", comp_id)
    status_text = st.sidebar.empty()
    progress_bar = st.sidebar.progress(0)
    if st.sidebar.button("Импорт данных", type="primary", disabled=True):
        if zone_exists(comp_id):
            progress_bar.progress(100)
            status_text.text("Зоны данной компании уже есть в базе. Импорт данных прерван.")
            time.sleep(0.05)
        else:
            status_text.text("Запрашиваем список групп геозон")
            time.sleep(0.05)
            df_groups = loadgroupzonelist(comp_id_sys)
            progress_bar.progress(10)
            status_text.text("Запрашиваем список геозон")
            time.sleep(0.05)
            df_zone = loadzonelist(sysnum, comp_id_sys, comp_id)
            progress_bar.progress(20)
            status_text.text("Вычисляем координаты групп")
            time.sleep(0.05)
            df_res = df_groups.merge(df_zone, left_on='id', right_on='id_group', how = 'left')
            progress_bar.progress(30)
            result = df_res.groupby(['id', 'name', 'companyId'], as_index=False).agg({'agro_area': 'sum',
                                                                                      'min_lat': 'min',
                                                                                      'max_lat': 'max',
                                                                                      'min_lon': 'min',
                                                                                      'max_lon': 'max',
                                                                                      'mean_lat': 'mean',
                                                                                      'mean_lon': 'mean'})
            time.sleep(0.05)
            progress_bar.progress(40)
            time.sleep(0.05)
            status_text.text("Проводим очистку данных")
            result.rename(columns={'id': 'group_sysid', 'name': 'group_name', 'companyId': 'id_company', 
                           'agro_area': 'dimension'}, inplace=True)
            df_group = result.dropna()
            df_group['id_address'] = None
            df_group = df_group.reindex(['id_company', 'group_name', 'id_address', 'min_lat', 'min_lon', 'max_lat','max_lon','group_sysid',
                     'mean_lat','mean_lon','dimension'],axis=1)
            df_group['id_company'] = comp_id
            progress_bar.progress(50)
            time.sleep(0.05)
            status_text.text("Сохраняем группы геозон в БД")
#Записываем группы в БД
            if group_insert(df_group):
                progress_bar.progress(60)
                time.sleep(0.05)
#Считываем объекты из БД для подстановки ID
                df_group = group_return(comp_id)
                df1 = df_group.drop(columns=['id_company', 'group_name', 'id_address', 'min_lat', 'min_lon', 'max_lat' , 
                                                 'max_lon', 'mean_lat', 'mean_lon', 'dimension'])
                df2 = df1.merge(df_zone, left_on='group_sysid', right_on='id_group', how='left')
                df2.drop(columns=['group_sysid', 'id_group'], inplace=True)
                df2.rename(columns={'id_agragrozone': 'id_group'}, inplace=True)
                df_zone = df2.reindex(['id_company', 'id_system', 'id_group', 'zone_name', 'agro_area', 'id_address', 'min_lat',
                                       'min_lon', 'max_lat', 'max_lon', 'address_zone', 'zone_sysid', 'mean_lat', 'mean_lon'], axis=1)
                if zone_insert(df_zone):
                    progress_bar.progress(100)
                    status_text.text("Импорт данных завершен")
                    time.sleep(0.05)
                    st.write(" ### Импорт данных завершен")
                else:
                    progress_bar.progress(100)
                    status_text.text("Сбой импорта данных")
                    time.sleep(0.05)
                    st.write(" ### Сбой импорта данных в зонах")
            else:
                progress_bar.progress(100)
                status_text.text("Сбой импорта данных")
                time.sleep(0.05)
                st.write(" ### Сбой импорта данных в группах")
                
            st.write("### Группы геозон", df_group)
            st.write("### Геозон", df_zone)