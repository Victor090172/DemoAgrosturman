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
#import altair as alt
#from sqlalchemy import create_engine

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

#Создаем боковое меню
st.set_page_config(page_title="Импорт данных", page_icon="📈")
st.markdown("# Импорт данных")
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
    'Вы выбрали: ', CompanyList
    id_company = df_company.loc[df_company['name'] == CompanyList]['id'].item()
    st.write("ID in Fort: ", id_company)
 #Предлагаем заполнить название и адрес
    if st.checkbox('Добавить полное наимнование компании (рекомендуется)'):
        fullname = st.text_input('Введите полное наименование компании (будет записано в БД)')
    if st.checkbox('Добавить адрес компании (рекомендуется)'):
        adrcompany = st.text_input('Введите адрес компании (понадобится в дальнейшем для отображения на карте)')
#Предлагаем выбрать дату скачивания  
    d = st.date_input("Введите дату с которой начнется импорт данных:", value=None)
    now = datetime.datetime.now().date()
    if d != None:
        st.write("Импорт данных в систему аналитики будет произведен начиная с ", d, " по ", now)
        if st.sidebar.button("Импорт данных", type="primary"):
            status_text = st.sidebar.empty()
            progress_bar = st.sidebar.progress(0)
            status_text.text("Формируем запрос к " + syslist)
            time.sleep(0.05)
            df_objects = loadobjectlist(id_company)
            progress_bar.progress(10)
            dict = df_objects['id'].to_string(index=False).split('\n')
            objects =";".join(str(element) for element in dict)
            status_text.text("Читаем данные из " + syslist)
            df_stst = loadobjectsstst(objects, d, now)
            progress_bar.progress(20)
            status_text.text("Данные считаны успешно")
            time.sleep(0.05)
            status_text.text("Начинаем очистку данных")
            drop_row_index = df_stst.loc[df_stst['isTotal']==True].index
            progress_bar.progress(30)
            time.sleep(0.05)
            df_stst.drop(drop_row_index, inplace=True)
            progress_bar.progress(40)
            time.sleep(0.05)
            st.write(" ### Список объектов", df_stst)
