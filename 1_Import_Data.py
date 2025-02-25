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