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
from sqlalchemy import create_engine

url = 'https://glonassagro.com/'
username = 'rvl_testapi'
password = 'rvltestapi2024'
API = 'api/integration/v1/'
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
def loaddata():
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
    df = loaddata()
    CompanyList = st.selectbox(
       'Выберите компанию (данные загружены по API из системы Форт Монитор):',
        df['name'].unique())
    'Вы выбрали: ', CompanyList
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
        st.button("Импорт данных", type="primary")