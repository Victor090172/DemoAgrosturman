# -*- coding: utf-8 -*-
"""
Created on Tue Apr  1 12:24:50 2025

@author: Agropilot-Project
"""

import streamlit as st
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import psycopg2
import datetime
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

#Возвращает список групп в БД
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

#Запрашиваем погоду на текущий день
def get_weather_today(latitude_list, longitude_list, zone_list):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
    	"latitude": latitude_list,
    	"longitude": longitude_list,
    	"hourly": ["temperature_2m", "precipitation", "weather_code", "relative_humidity_2m", "dew_point_2m", 
                "wind_speed_10m", "wind_direction_10m", "soil_temperature_0cm", "soil_temperature_6cm", 
                "soil_temperature_18cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", 
                "soil_moisture_9_to_27cm", "precipitation_probability", "pressure_msl", "surface_pressure"],
        "forecast_days": 1
        }
    responses = openmeteo.weather_api(url, params=params)
# Process first location. Add a for-loop for multiple locations or weather models
    result = pd.DataFrame()
    for i in range(len(responses)):
        resp = responses[i]
        hourly = resp.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(3).ValuesAsNumpy()
        hourly_dew_point_2m = hourly.Variables(4).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(5).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(6).ValuesAsNumpy()
        hourly_soil_temperature_0cm = hourly.Variables(7).ValuesAsNumpy()
        hourly_soil_temperature_6cm = hourly.Variables(8).ValuesAsNumpy()
        hourly_soil_temperature_18cm = hourly.Variables(9).ValuesAsNumpy()
        hourly_soil_moisture_0_to_1cm = hourly.Variables(10).ValuesAsNumpy()
        hourly_soil_moisture_1_to_3cm = hourly.Variables(11).ValuesAsNumpy()
        hourly_soil_moisture_3_to_9cm = hourly.Variables(12).ValuesAsNumpy()
        hourly_soil_moisture_9_to_27cm = hourly.Variables(13).ValuesAsNumpy()
        hourly_precipitation_probability = hourly.Variables(14).ValuesAsNumpy()
        hourly_pressure_msl = hourly.Variables(15).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(16).ValuesAsNumpy()

        hourly_data = {"datetime": pd.date_range(
                    	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                    	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                    	freq = pd.Timedelta(seconds = hourly.Interval()),
                    	inclusive = "left"
                        )}
        hourly_data['id_group'] = zone_list[i]
#        hourly_data['lat'] = resp.Latitude()
#        hourly_data['lon'] = resp.Longitude() 
        hourly_data['lat'] = latitude_list[i]
        hourly_data['lon'] = longitude_list[i]
        
        
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["dew_point_2m"] = hourly_dew_point_2m
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
        hourly_data["soil_temperature_0cm"] = hourly_soil_temperature_0cm
        hourly_data["soil_temperature_6cm"] = hourly_soil_temperature_6cm
        hourly_data["soil_temperature_18cm"] = hourly_soil_temperature_18cm
        hourly_data["soil_moisture_0_to_1cm"] = hourly_soil_moisture_0_to_1cm
        hourly_data["soil_moisture_1_to_3cm"] = hourly_soil_moisture_1_to_3cm
        hourly_data["soil_moisture_3_to_9cm"] = hourly_soil_moisture_3_to_9cm
        hourly_data["soil_moisture_9_to_27cm"] = hourly_soil_moisture_9_to_27cm
        hourly_data["precipitation_probability"] = hourly_precipitation_probability
        hourly_data["pressure_msl"] = hourly_pressure_msl
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_dataframe = pd.DataFrame(data = hourly_data)
        result = pd.concat([result, hourly_dataframe], ignore_index=True)
    return result

#Запрашиваем прогноз погоды на 7 дней
def get_weather_forecast(latitude_list, longitude_list, zone_list):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
    	"latitude": latitude_list,
    	"longitude": longitude_list,
    	"hourly": ["temperature_2m", "precipitation", "weather_code", "relative_humidity_2m", "dew_point_2m", 
                "wind_speed_10m", "wind_direction_10m", "soil_temperature_0cm", "soil_temperature_6cm", 
                "soil_temperature_18cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", 
                "soil_moisture_9_to_27cm", "precipitation_probability", "pressure_msl", "surface_pressure"],
        "forecast_days": 7
        }
    responses = openmeteo.weather_api(url, params=params)
# Process first location. Add a for-loop for multiple locations or weather models
    result = pd.DataFrame()
    for i in range(len(responses)):
        resp = responses[i]
        hourly = resp.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(3).ValuesAsNumpy()
        hourly_dew_point_2m = hourly.Variables(4).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(5).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(6).ValuesAsNumpy()
        hourly_soil_temperature_0cm = hourly.Variables(7).ValuesAsNumpy()
        hourly_soil_temperature_6cm = hourly.Variables(8).ValuesAsNumpy()
        hourly_soil_temperature_18cm = hourly.Variables(9).ValuesAsNumpy()
        hourly_soil_moisture_0_to_1cm = hourly.Variables(10).ValuesAsNumpy()
        hourly_soil_moisture_1_to_3cm = hourly.Variables(11).ValuesAsNumpy()
        hourly_soil_moisture_3_to_9cm = hourly.Variables(12).ValuesAsNumpy()
        hourly_soil_moisture_9_to_27cm = hourly.Variables(13).ValuesAsNumpy()
        hourly_precipitation_probability = hourly.Variables(14).ValuesAsNumpy()
        hourly_pressure_msl = hourly.Variables(15).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(16).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
                    	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                    	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                    	freq = pd.Timedelta(seconds = hourly.Interval()),
                    	inclusive = "left"
                        )}
        hourly_data['id_group'] = zone_list[i]
#        hourly_data['lat'] = resp.Latitude()
#        hourly_data['lon'] = resp.Longitude() 
        hourly_data['lat'] = latitude_list[i]
        hourly_data['lon'] = longitude_list[i]
        
        
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["dew_point_2m"] = hourly_dew_point_2m
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
        hourly_data["soil_temperature_0cm"] = hourly_soil_temperature_0cm
        hourly_data["soil_temperature_6cm"] = hourly_soil_temperature_6cm
        hourly_data["soil_temperature_18cm"] = hourly_soil_temperature_18cm
        hourly_data["soil_moisture_0_to_1cm"] = hourly_soil_moisture_0_to_1cm
        hourly_data["soil_moisture_1_to_3cm"] = hourly_soil_moisture_1_to_3cm
        hourly_data["soil_moisture_3_to_9cm"] = hourly_soil_moisture_3_to_9cm
        hourly_data["soil_moisture_9_to_27cm"] = hourly_soil_moisture_9_to_27cm
        hourly_data["precipitation_probability"] = hourly_precipitation_probability
        hourly_data["pressure_msl"] = hourly_pressure_msl
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_dataframe = pd.DataFrame(data = hourly_data)
        result = pd.concat([result, hourly_dataframe], ignore_index=True)
    return result

#Проверка на наличие данных по погоде на текущуюд дату в БД
@st.cache_data
def weather_exists():
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
        cur.execute("select exists(select * from weather_today where create_date=NOW()::date);")
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return exists

#Загрузка сегодняшней погоды в БД
@st.cache_data
def weather_today_insert(df):
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
                COPY weather_today (
                        id_group,
                        datetime,
                        lat,
                        lon,
                        temperature_2m,
                        relative_humidity_2m,
                        dew_point_2m,
                        wind_speed_10m,
                        wind_direction_10m,
                        soil_temperature_0cm,
                        soil_temperature_6cm,
                        soil_temperature_18cm,
                        soil_moisture_0_to_1cm,
                        soil_moisture_1_to_3cm,
                        soil_moisture_3_to_9cm,
                        soil_moisture_9_to_27cm,
                        pressure_msl,
                        surface_pressure,
                        precipitation_probability,
                        precipitation,
                        create_date,
                        weather_code
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

#Загрузка прогноза погоды в БД
@st.cache_data
def weather_forecast_insert(df):
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
                COPY weather_forecast (
                        id_group,
                        create_date,
                        date,
                        lat,
                        lon,
                        temperature_2m,
                        relative_humidity_2m,
                        dew_point_2m,
                        wind_speed_10m,
                        wind_direction_10m,
                        soil_temperature_0cm,
                        soil_temperature_6cm,
                        soil_temperature_18cm,
                        soil_moisture_0_to_1cm,
                        soil_moisture_1_to_3cm,
                        soil_moisture_3_to_9cm,
                        soil_moisture_9_to_27cm,
                        pressure_msl,
                        surface_pressure,
                        precipitation_probability,
                        precipitation,
                        weather_code
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
st.set_page_config(page_title="Прогноз погоды", page_icon="⛅")
st.markdown("# Прогноз погоды")
st.sidebar.header("Прогноз погоды")
dictsys = load_table_system()
df_company = load_table_company()
CompanyList = st.selectbox(
   'Выберите компанию:',
    df_company['short_name'].unique())
comp_id = df_company.loc[df_company['short_name'] == CompanyList, 'id_company'].item()
comp_id_sys = df_company.loc[df_company['short_name'] == CompanyList, 'id_insys'].item()
st.write("Вы выбрали ", CompanyList)
st.write("ID в Форт ", comp_id_sys)
st.write("ID в БД ", comp_id)
if st.sidebar.button("Запросить погоду", type="primary", disabled=False):
    if weather_exists():
        st.markdown("# Данные на сегодня уже есть в БД")
    else:
        now = datetime.datetime.now().date()
        #Читаем группы геозон из БД
        df_group = group_return(comp_id)
        zone_list = df_group['id_agragrozone'].to_list()
        latitude_list = df_group['mean_lat'].to_list()
        longitude_list = df_group['mean_lon'].to_list()
        #Получаем погоду на сегодня
        df_twather = get_weather_today(latitude_list, longitude_list, zone_list)
        df_twather['create_date'] = now
        df_twather = df_twather.reindex(['id_group', 'datetime', 'lat', 'lon', 'temperature_2m', 'relative_humidity_2m',
                                         'dew_point_2m', 'wind_speed_10m', 'wind_direction_10m', 'soil_temperature_0cm', 
                                         'soil_temperature_6cm', 'soil_temperature_18cm', 'soil_moisture_0_to_1cm', 
                                         'soil_moisture_1_to_3cm', 'soil_moisture_3_to_9cm', 'soil_moisture_9_to_27cm', 'pressure_msl',
                                         'surface_pressure', 'precipitation_probability', 'precipitation', 
                                         'create_date', 'weather_code'], axis=1)
        #Получаем погоду на 7 дней
        df_fwather = get_weather_forecast(latitude_list, longitude_list, zone_list)
        df_fwather['create_date'] = now
        df_fwather = df_fwather.reindex(['id_group', 'create_date', 'date', 'lat', 'lon', 'temperature_2m', 'relative_humidity_2m',
                                         'dew_point_2m', 'wind_speed_10m', 'wind_direction_10m', 'soil_temperature_0cm', 
                                         'soil_temperature_6cm', 'soil_temperature_18cm', 'soil_moisture_0_to_1cm', 
                                         'soil_moisture_1_to_3cm', 'soil_moisture_3_to_9cm', 'soil_moisture_9_to_27cm', 'pressure_msl',
                                         'surface_pressure', 'precipitation_probability', 'precipitation', 'weather_code'], axis=1)
#Загрузка погодына сегодня в БД       
        if weather_today_insert(df_twather):
            st.write(" ### Импорт данных завершен")
        else:
            st.write(" ### Сбой импорта данных в зонах")
# Загрузка прогноза погоды в БД           
        if weather_forecast_insert(df_fwather):
            st.write(" ### Импорт данных завершен")
        else:
            st.write(" ### Сбой импорта данных в зонах")
        st.write("### Группы геозон", df_group)
        st.write("### Погода не сегодня", df_twather)
        st.write("### Прогноз не 7 дней", df_fwather)