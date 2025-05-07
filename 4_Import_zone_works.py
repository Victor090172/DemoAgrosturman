# -*- coding: utf-8 -*-
"""
Created on Tue Apr 29 13:19:01 2025

@author: Agropilot-Project
"""
import streamlit as st
import pandas as pd
import psycopg2
import httpx
import datetime
import time
import json
from io import StringIO
import re
import warnings
warnings.filterwarnings('ignore')
import logging

logging.basicConfig(level=logging.INFO)

#Вставка работ по объектам в бд
@st.cache_data
def objects_work_insert(df):
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
                COPY objects_work (
                    id_company, 
                    id_object, 
                    start_period, 
                    stop_period, 
                    fuel, 
                    moto_hours, 
                    run_fuel,
                    run_duration_hours, 
                    run_stop_hours,
                    run_distance,
                    run_fuel_per100km,
                    run_fuel_per1hour,
                    run_avg_speed,
                    run_square,
                    run_square_percent,
                    run_fuel_per1ha,
                    work_fuel,
                    work_duration,
                    work_stop_hours,
                    work_dist,
                    work_fuel_per100km,
                    work_fuel_per1hour,
                    work_avg_speed,
                    work_square,
                    work_square_percent,
                    work_fuel_per1ha
                    ) FROM STDIN WITH CSV""",
                file=sio
            )
        conn.commit()
        flag = True
    except psycopg2.Error as e:
        print (e)
        conn.rollback()
        flag = False
    conn.close()
    return flag    


#Вставка работ в геохонах в бд
@st.cache_data
def zone_work_insert(df):
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
                COPY objects_work_zone (
                    id_company, 
                    id_object, 
                    id_trailer, 
                    id_zone, 
                    start_visit, 
                    stop_visit, 
                    all_fuel,
                    fuel_on_hh, 
                    duration_hours,
                    stops_hours,
                    move_hours,
                    distance,
                    fuel_per100km,
                    fuel_per1hour,
                    avg_speed,
                    square,
                    square_percent,
                    fuel_per1ha,
                    moto_hours,
                    moto_hours_on_hh
                    ) FROM STDIN WITH CSV""",
                file=sio
            )
        conn.commit()
        flag = True
    except psycopg2.Error as e:
        print (e)
        conn.rollback()
        flag = False
    conn.close()
    return flag    

#Сохранение тревог
@st.cache_data
def alarm_insert(df):
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
                COPY objects_work_alarms (
                    id_company, 
                    id_object, 
                    id_zone, 
                    id_trailer,
                    start_alarm,
                    stop_alarm,
                    max_speed,
                    distance
                    ) FROM STDIN WITH CSV""",
                file=sio
            )
        conn.commit()
        flag = True
    except psycopg2.Error as e:
        print (e)
        conn.rollback()
        flag = False
    conn.close()
    return flag    


#Обновление списка орудий
@st.cache_data
def trailer_insert(df):
# Устанавливаем подключение к базе данных PostgreSQL
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password="AgroPilot2025",
        host="82.142.178.174",
        port='5432'
    ) 
    conn.autocommit = False  # Убедитесь, что autocommit выключен
    flag = False
    sio = StringIO()
    df.to_csv(sio, index=None, header=None)
    sio.seek(0)
    try:
        with conn.cursor() as c:
            c.copy_expert(
                sql="""
                COPY trailers (
                    id_company, 
                    sysid, 
                    traler_name, 
                    trailer_width
                    ) FROM STDIN WITH CSV""",
                file=sio
            )
        conn.commit()
        logging.info("Данные успешно записаны")
        flag = True
    except psycopg2.Error as e:
        logging.error(f"Ошибка при записи: {e}")
        print (f"Ошибка при записи: {e}")
        conn.rollback()
        flag = False
    finally:
        conn.close()
    return flag    


#Обновление списка культур
@st.cache_data
def culture_insert(df):
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
                COPY culture (
                    id_company, 
                    culture_name, 
                    year, 
                    id_zone
                    ) FROM STDIN WITH CSV""",
                file=sio
            )
        conn.commit()
        flag = True
    except psycopg2.Error as e:
        print (e)
        conn.rollback()
        flag = False
    conn.close()
    return flag    


#Возвращает число месяцев между датами
@st.cache_data
def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


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

#Возвращает список объектов в БД
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

#Возвращает список орудий
@st.cache_data
def trailers_return(id_company):
    conn = psycopg2.connect(
               dbname="postgres",
                user="postgres",
                password="AgroPilot2025",
                host="82.142.178.174",
                port="5432",
                )
    id_company = str(id_company)
    try:
        cur = conn.cursor()
        sql = f"SELECT * FROM trailers WHERE id_company = '{id_company}';"
        dat = pd.read_sql_query(sql, conn)
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return dat

@st.cache_data
def trailers_return1(id_company):
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='AgroPilot2025',
        host='82.142.178.174',
        port='5432'
    )
    conn.autocommit = True
    try:
        # Вариант 1: Через cursor (без Pandas)
        cur = conn.cursor()
        cur.execute("SELECT * FROM public.trailers WHERE id_company = %s;", (id_company,))
        rows = cur.fetchall()
        if rows:
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=cols)
        
        # Вариант 2: Через Pandas (если пусто)
        return pd.read_sql_query(
            "SELECT * FROM public.trailers WHERE id_company = %s;",
            conn,
            params=(id_company,)
        )
    except Exception as e:
        print(f"Ошибка: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


#Возвращает список агрозон
@st.cache_data
def zone_return(id_company):
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
        sql = "select * from agrozones where id_company= " + id_company+";"
        dat = pd.read_sql_query(sql, conn)
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return dat


#Возвращает список культур
@st.cache_data
def culture_return(id_company):
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
        sql = "select * from culture where id_company= " + id_company+";"
        dat = pd.read_sql_query(sql, conn)
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print (e)
    return dat


#Определяем тип объекта
@st.cache_data
def return_type (obj_name):
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
def loadobjectlist(id_company):
    url = 'https://glonassagro.com/'
    username = 'rvl_testapi'
    password = 'rvltestapi2024'
    API = 'api/integration/v1/'
    method = 'connect'
    headers = {'Content-Type' : 'application/json'}
    data = {'login' : username,
            'password' : password,
            'lang' : 'ru-ru',
            'timezone' : '0'}
    client = httpx.Client()
    response = client.post(url+API+method, timeout=1000, headers=headers, json=data)
    sessionid = response.headers['SessionId']
    headers = {'SessionId':sessionid}
    path = 'getobjectslist'
    query = {'companyId': id_company}
    resp = client.get(url+API+path, headers=headers, params=query)
    df = pd.DataFrame.from_dict(resp.json()['objects'])
    df['id_type'] = df.apply(lambda x: return_type(x['name']), axis=1)
    method = 'disconnect'
    response = client.get(url+API+method, timeout=1000)
    return df

@st.cache_data
def loadobjectsgeo(objects_id, start_date, stop_date):
    url = 'https://glonassagro.com/'
    username = 'rvl_testapi'
    password = 'rvltestapi2024'
    API = 'api/integration/v1/'
    method = 'connect'
    headers = {'Content-Type' : 'application/json'}
    data = {'login' : username,
            'password' : password,
            'lang' : 'ru-ru',
            'timezone' : '0'}
    objects =",".join(str(element) for element in objects_id)
    path = 'getagroreport'
    client = httpx.Client()
    response = client.post(url+API+method, timeout=1000, headers=headers, json=data)
    sessionid = response.headers['SessionId']
    headers = {'SessionId':sessionid,
          'Content-Type' : 'application/json'}
    query = {'start': start_date,
            'stop': stop_date,
            'oids': objects,
            'split':'day',
            'groupByShifts': False,
            'disableOverlayTrack': False,
            'minWorkedArea': 1,
            'minZoneVisitMinutes': 5,
            'minTimeBetweenZoneVisits': 5}
    resp = client.get(url+API+path, timeout=5000, headers=headers, params=query)
    obj_dict = resp.json()
    # Преобразуем данные в список словарей Python
    try:
        if isinstance(obj_dict, str):  # Если данные — строка JSON
            parsed_data = json.loads(obj_dict)
        elif isinstance(obj_dict, list):  # Если данные уже список
            parsed_data = obj_dict
        else:
            raise ValueError("Неподдерживаемый формат данных")
    except json.JSONDecodeError as e:
        print(f"Ошибка при парсинге JSON: {e}")
        parsed_data = []
    # Проверяем тип данных
    if not isinstance(parsed_data, list):
        raise ValueError("Данные должны быть списком словарей")
    method = 'disconnect'
    response = client.get(url+API+method, timeout=1000)
    # Создаем основные таблицы
    trailer_info = []
    zone_info = []
    work_info = []
    zone_visits = []
    speed_alarms = []
    # Обрабатываем каждую запись
    for record in parsed_data:
        # Информация об объекте
        object_id = record.get("objectInfo", {}).get("objectID")
        
        # Обработка периодов работы (periodsInfo)
        periods = record.get("periodsInfo", [])
        for period in periods:
            # Информация о поездках
            start_period = period.get("startPeiod")
            stop_period = period.get("stopPeiod")
            fuel = period.get("fuel")
            moto_hours = period.get("motoHours")
            fuel_run = period.get("runInfo", {}).get("fuel")
            duration_run = period.get("runInfo", {}).get("durationHours")
            stop_hours = period.get("runInfo", {}).get("stopsHours")
            dist = period.get("runInfo", {}).get("distance")
            fuelPer100Km =  period.get("runInfo", {}).get('fuelPer100Km')
            fuelPer1Hour = period.get("runInfo", {}).get('fuelPer1Hour')
            avgSpeed = period.get("runInfo", {}).get('avgSpeed')
            square = period.get("runInfo", {}).get('square')
            squarePercent = period.get("runInfo", {}).get('squarePercent')
            fuelPer1Ha = period.get("runInfo", {}).get('fuelPer1Ha')
            fuel_work = period.get("workInfo", {}).get('fuel')
            work_duration = period.get("workInfo", {}).get("durationHours")
            work_stop_hours = period.get("workInfo", {}).get("stopsHours")
            work_dist = period.get("workInfo", {}).get("distance")
            work_fuelPer100Km =  period.get("workInfo", {}).get('fuelPer100Km')
            work_fuelPer1Hour = period.get("workInfo", {}).get('fuelPer1Hour')
            work_avgSpeed = period.get("workInfo", {}).get('avgSpeed')
            work_square = period.get("workInfo", {}).get('square')
            work_squarePercent = period.get("workInfo", {}).get('squarePercent')
            work_fuelPer1Ha = period.get("workInfo", {}).get('fuelPer1Ha')
            work_info.append({
                "objectID" : object_id,
                "start_period" : start_period,
                "stop_period" : stop_period,
                "fuel" : fuel,
                "moto_hours" : moto_hours,
                "run_fuel" : fuel_run,
                "run_duration_hours": duration_run,
                "run_stop_hours" : stop_hours,
                "run_distance" : dist,
                "run_fuel_per100km" : fuelPer100Km,
                "run_fuel_per1hour" : fuelPer1Hour,
                "run_avg_speed" : avgSpeed,
                "run_square" : square,
                "run_square_percent" : squarePercent,
                "run_fuel_per1ha" : fuelPer1Ha,
                "work_fuel" : fuel_work,
                "work_duration" : work_duration,
                "work_stop_hours" : work_stop_hours,
                "work_dist" : work_dist,
                "work_fuel_per100km" : work_fuelPer100Km,
                "work_fuel_per1hour" : work_fuelPer1Hour,
                "work_avg_speed" : work_avgSpeed,
                "work_square" :work_square,
                "work_square_percent" : work_squarePercent,
                "work_fuel_per1ha" : work_fuelPer1Ha
            })
    
            
            # Информация о трейлере
            trailer_id = period.get("trailerId")
            trailer_name = period.get("trailerName")
            trailer_width = period.get("trailerWidth")
            trailer_info.append({
                "trailerId": trailer_id,
                "trailerName": trailer_name,
                "trailerWidth": trailer_width,
            })
            
            # Информация о зоне
            zones = period.get("zonesInfo", [])
            for zone in zones:
                zone_id = zone.get("zoneInfo", {}).get("id")
                zone_name = zone.get("zoneInfo", {}).get("zoneName")
                culture_name = zone.get("zoneInfo", {}).get("cultureName")
                square = zone.get("zoneInfo", {}).get("square")
                zone_info.append({
                    "zoneId": zone_id,
                    "zoneName": zone_name,
                    "year" : start_period,
                    "cultureName": culture_name,
                    "square": square
                })
                zone_visit = zone.get("zoneVisits", [])
                for visit in zone_visit:
                    start_visit = visit.get("start")
                    stop_visit = visit.get("stop")
                    all_fuel = visit.get("workInfo", {}).get("fuel")
                    fuelOnHH = visit.get("workInfo", {}).get("fuelOnHH")
                    durationHours = visit.get("workInfo", {}).get("durationHours")
                    stopsHours = visit.get("workInfo", {}).get("stopsHours")
                    moveHours = visit.get("workInfo", {}).get("moveHours")
                    distance = visit.get("workInfo", {}).get("distance")
                    fuelPer100Km = visit.get("workInfo", {}).get("fuelPer100Km")
                    fuelPer1Hour = visit.get("workInfo", {}).get("fuelPer1Hour")
                    avgSpeed = visit.get("workInfo", {}).get("avgSpeed")
                    square = visit.get("workInfo", {}).get("square")
                    squarePercent = visit.get("workInfo", {}).get("squarePercent")
                    fuelPer1Ha = visit.get("workInfo", {}).get("fuelPer1Ha")
                    motoHours = visit.get("workInfo", {}).get("motoHours")
                    motoHoursOnHH = visit.get("workInfo", {}).get("motoHoursOnHH")
                    zone_visits.append({
                        "ID_Object" : object_id,
                        "ID_Traler" : trailer_id,
                        "ID_Zone" : zone_id,
                        "start_visit" : start_visit,
                        "stop_visit" : stop_visit,
                        "all_fuel" : all_fuel,
                        "fuel_on_hh": fuelOnHH,
                        "duration_hours" : durationHours,
                        "stops_hours" : stopsHours,
                        "move_hours" : moveHours,
                        "distance" : distance,
                        "fuel_per100km" : fuelPer100Km,
                        "fuel_per1hour" : fuelPer1Hour,
                        "avg_speed" : avgSpeed,
                        "square" : square,
                        "square_percent" : squarePercent,
                        "fuel_per1ha" : fuelPer1Ha,
                        "moto_hours" : motoHours,
                        "moto_hours_on_hh" : motoHoursOnHH
                    })
                    speedAlarm = visit.get("SpeedAlarms", [])
                    for alarm in speedAlarm:
                        start_alarm = alarm.get("start")
                        stop_alarm = alarm.get("stop")
                        maxSpeed = alarm.get("maxSpeed")
                        distance = alarm.get("distance")
                        speed_alarms.append({
                                            "ID_Object" : object_id,
                                            "ID_Traler" : trailer_id,
                                            "ID_Zone" : zone_id,
                                            "start_alarm" : start_alarm,
                                            "stop_alarm": stop_alarm,
                                            "max_speed" : maxSpeed,
                                            "distance" : distance
                                            })
    work_info_df =  pd.DataFrame(work_info).drop_duplicates().reset_index(drop=True)
    trailer_info_df = pd.DataFrame(trailer_info).drop_duplicates().reset_index(drop=True)
    zone_info_df = pd.DataFrame(zone_info).drop_duplicates().reset_index(drop=True)
    zone_visits_df = pd.DataFrame(zone_visits).drop_duplicates().reset_index(drop=True)
    alarm_df = pd.DataFrame(speed_alarms).drop_duplicates().reset_index(drop=True)
    return work_info_df, trailer_info_df, zone_info_df, zone_visits_df, alarm_df

#Создаем боковое меню
st.set_page_config(page_title="Импорт работ", page_icon="📈")
st.markdown("# Импорт работ объектов в геозонах за период")
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
    df_company = load_table_company()
    CompanyList = st.selectbox(
       'Выберите компанию:',
        df_company['short_name'].unique())
    comp_id = df_company.loc[df_company['short_name'] == CompanyList, 'id_company'].item()
    comp_id_sys = df_company.loc[df_company['short_name'] == CompanyList, 'id_insys'].item()
    st.write("Вы выбрали ", CompanyList)
    st.write("ID в Форт ", comp_id_sys)
    st.write("ID в БД ", comp_id)
#Предлагаем выбрать дату скачивания  
    d = st.date_input("Введите дату с которой начнется импорт данных:", value=None)
    now = datetime.datetime.now().date()
    if d != None:
        st.write("Импорт данных в систему аналитики будет произведен начиная с ", d, " по ", now)
        if st.sidebar.button("Импорт данных", type="primary",   disabled=True):
            status_text = st.sidebar.empty()
            progress_bar = st.sidebar.progress(0)
            status_text.text("Читаем список объектов")
            progress_bar.progress(5)
            time.sleep(0.1)
            df_object = objects_return(comp_id)
            st.write(" ### Список объектов")
            df_object
            status_text.text("Выбираем агро-объекты")
            progress_bar.progress(5)
            time.sleep(0.1)
            objects_df = df_object.loc[df_object['id_type'].isin([4,5,6,7,8])]
            objects_id = objects_df['oid'].to_string(index=False).split('\n')
            status_text.text("Заправшиваем перечень работ в агрозонах в системе ")
            progress_bar.progress(5)
            time.sleep(0.1)
            df1, df2, df3, df4, df5 = loadobjectsgeo(objects_id, d, now)
            #Работа объектов
            status_text.text("Делаем выборку работ объектов")
            progress_bar.progress(10)
            time.sleep(0.1)
            df_object_work = df_object.merge(df1, left_on='oid', right_on='objectID', how= 'inner')
            df_object_work.drop(columns=['id_sys', 'object_name', 'id_model', 'id_type', 'reg_number', 'imei', 'status', 'last_date',
                                        'oid', 'objectID'], inplace=True)
            df_object_work = df_object_work.rename(columns={'id_objects': 'id_object'}).reindex(columns=['id_company', 'id_object', 'start_period',
                                                                                                        'stop_period', 'fuel', 'moto_hours','run_fuel',
                                                                                                         'run_duration_hours','run_stop_hours','run_distance',
                                                                                                        'run_fuel_per100km','run_fuel_per1hour','run_avg_speed',
                                                                                                        'run_square','run_square_percent','run_fuel_per1ha',
                                                                                                        'work_fuel','work_duration','work_stop_hours','work_dist',
                                                                                                        'work_fuel_per100km','work_fuel_per1hour','work_avg_speed',
                                                                                                        'work_square','work_square_percent','work_fuel_per1ha'])
            #Сохраняем работы в БД
            status_text.text("Сохраняем работы объектов в БД")
            progress_bar.progress(15)
            time.sleep(0.1)
            if objects_work_insert(df_object_work):
                status_text.text("Работы по объектам сохраненты в БД")
                progress_bar.progress(20)
                time.sleep(0.1)
                st.write(" ### Работа объектов")
                df_object_work
            else:
                status_text.text("Проблемы с сохранением работ")
                progress_bar.progress(20)
                time.sleep(0.1)
            #Орудия
            status_text.text("Обновляем список орудий")
            progress_bar.progress(25)
            time.sleep(0.1)            
            df_trailer = trailers_return(comp_id)
            df_trailer_new = df_trailer.merge(df2, left_on='sysid', right_on='trailerId', how= 'right')
            df_trailer_new = df_trailer_new.loc[df_trailer_new['id_trailer'].isna()]
            df_trailer_new['id_company'] = comp_id
            df_trailer_new = df_trailer_new.drop(columns=['id_trailer', 'sysid', 'traler_name', 'trailer_width'])\
                                           .rename(columns={'trailerId':'sysid', 'trailerName': 'traler_name', 'trailerWidth': 'trailer_width'})
             #Запись орудий в базу
            status_text.text("Добавляем новые орудия в базу")
            progress_bar.progress(25)
            time.sleep(0.1)
            if trailer_insert(df_trailer_new):
                time.sleep(2)  # Ждем 2 секунды для гарантии видимости данных
                st.write(comp_id)
                df_trailer = trailers_return(comp_id)
                status_text.text("Орудия успешно добавлены")
                progress_bar.progress(30)
                time.sleep(0.1)
                st.write(" ### Список орудий")
                max_attempts = 10
                attempt = 0
                while len(df_trailer) == 0 and attempt < max_attempts:
                    logging.info(f"Попытка {attempt + 1} чтения данных...")
                    df_trailer = trailers_return1(comp_id)
                    if len(df_trailer) == 0:
                        time.sleep(2)
                    attempt += 1
                df_trailer
            else:
                status_text.text("Проблемы с добавлением орудий")
                progress_bar.progress(30)
                time.sleep(0.1)
                logging.error("Не удалось записать данные")
            st.write(comp_id)
            df_trailer1 = trailers_return1(comp_id)
            st.write(" ### Список орудий")
            df_trailer1
            #Культуры
            status_text.text("Обновляем список культур")
            progress_bar.progress(35)
            time.sleep(0.1)
            df_zone = zone_return(comp_id)
            df_zone = df_zone.reindex(columns=['id_agzone', 'zone_sysid'])
            df3['year'] = pd.to_datetime(df3['year']).dt.year
            df_culture=culture_return(comp_id)
            df_culture_new = df_zone.merge(df3, left_on='zone_sysid', right_on='zoneId', how= 'inner')
            df_culture_new.drop_duplicates(subset=['id_agzone'], inplace=True)
            df_culture_new = df_culture_new.drop(columns=['zone_sysid', 'zoneId', 'zoneName', 'square']).rename(columns={'id_agzone': 'id_zone',
                                                                                                                'cultureName': 'culture_name'})
            df_culture_new = df_culture.merge(df_culture_new, left_on='id_zone', right_on='id_zone', how= 'right')
            df_culture_new = df_culture_new.loc[df_culture_new['id_culture'].isna()]
            df_culture_new['id_company'] = comp_id
            df_culture_new = df_culture_new.drop(columns=['id_culture', 'culture_name_x', 'year_x']).rename(columns={'year_y': 'year',
                                                                                                                    'culture_name_y': 'culture_name'})
            df_culture_new = df_culture_new.reindex(columns=['id_company', 'culture_name', 'year', 'id_zone'])
            #Сохранение списка в БД
            status_text.text("Сохраняем культуры в базу данных")
            progress_bar.progress(40)
            time.sleep(0.1)
            if culture_insert(df_culture_new):
                status_text.text("Культуры сохранены в БД")
                progress_bar.progress(45)
                time.sleep(0.1)
                st.write(" ### Список культур")
                df_culture_new
            else:
                status_text.text("Проблемы с сохранением культур")
                progress_bar.progress(45)
                time.sleep(0.1)
            status_text.text("Обрабатываем работу в геозонах и тревоги")
            progress_bar.progress(50)
            time.sleep(0.1) 
            #Работы в геозонах и тревоги
            df_work_zone = df_object.merge(df4, left_on='oid', right_on='ID_Object', how= 'inner')
            df_alarms = df_object.merge(df5, left_on='oid', right_on='ID_Object', how= 'inner')
            progress_bar.progress(55)
            time.sleep(0.1) 
            df_work_zone.drop(columns=['id_sys', 'object_name', 'id_model', 'id_type', 'reg_number', 'imei', 'status', 'last_date',
                                        'oid', 'ID_Object'], inplace=True)
            df_alarms.drop(columns=['id_sys', 'object_name', 'id_model', 'id_type', 'reg_number', 'imei', 'status', 'last_date',
                                    'oid', 'ID_Object'], inplace=True)
            df_work_zone = df_work_zone.merge(df_zone, left_on = 'ID_Zone', right_on = 'zone_sysid', how = 'right').dropna(subset=['id_objects'])
            df_alarms = df_alarms.merge(df_zone, left_on = 'ID_Zone', right_on = 'zone_sysid', how = 'right').dropna(subset=['id_objects'])
            df_work_zone.drop(columns=['ID_Zone', 'zone_sysid'], inplace=True)
            df_alarms.drop(columns=['ID_Zone', 'zone_sysid'], inplace=True)
            progress_bar.progress(60)
            time.sleep(0.1) 
            df_work_zone = df_work_zone.rename(columns={'id_agzone':'id_zone', 'id_objects': 'id_object'})
            df_alarms = df_alarms.rename(columns={'id_agzone':'id_zone', 'id_objects': 'id_object'})
            #Считывание орудий из базы для заполнения работы в геозонах
            #df_trailer = trailers_return(comp_id)
            df_trailer.drop(columns=['id_company', 'traler_name', 'trailer_width'], inplace=True)
            df_work_zone = df_work_zone.merge(df_trailer, left_on = 'ID_Traler', right_on = 'sysid', how = 'left')
            df_alarms = df_alarms.merge(df_trailer, left_on = 'ID_Traler', right_on = 'sysid', how = 'left')
            progress_bar.progress(65)
            time.sleep(0.1) 
            df_work_zone.drop(columns=['ID_Traler', 'sysid'], inplace=True)
            df_alarms.drop(columns=['ID_Traler', 'sysid'], inplace=True)
            df_work_zone = df_work_zone.reindex(columns=['id_company', 'id_object', 'id_trailer', 'id_zone', 'start_visit', 'stop_visit',
                                                        'all_fuel', 'fuel_on_hh', 'duration_hours', 'stops_hours', 'move_hours', 'distance',
                                                        'fuel_per100km', 'fuel_per1hour', 'avg_speed', 'square', 'square_percent', 'fuel_per1ha',
                                                        'moto_hours', 'moto_hours_on_hh'])

            df_alarms = df_alarms.reindex(columns=['id_company', 'id_object', 'id_zone', 'id_trailer', 'start_alarm', 'stop_alarm', 'max_speed',
                                                   'distance'])
            df_work_zone= df_work_zone.fillna(0)
            df_alarms=df_alarms.fillna(0)
            df_work_zone = df_work_zone.astype({'id_company':int, 'id_object':int, 'id_trailer':int, 'id_zone':int})
            df_alarms = df_alarms.astype({'id_company':int, 'id_object':int, 'id_trailer':int, 'id_zone':int})
            status_text.text("Сохраняем работы в БД")
            progress_bar.progress(70)
            time.sleep(0.1)
            if zone_work_insert(df_work_zone):
                status_text.text("Работы в зонах сохранены")
                progress_bar.progress(80)
                time.sleep(0.1)
                st.write(" ### Работы в геозонах")
                df_work_zone
            else:
                status_text.text("Проблемы с сохранением работ в зонах")
                progress_bar.progress(80)
                time.sleep(0.1)
            status_text.text("Сохраняем тревоги в БД")
            progress_bar.progress(90)
            time.sleep(0.1)
            if alarm_insert(df_alarms):
                status_text.text("Тревоги в зонах сохранены")
                progress_bar.progress(100)
                time.sleep(0.1)
                st.write(" ### Тревоги")
                df_alarms
            else:
                status_text.text("Проблемы при сохранении тревог")
                progress_bar.progress(100)
                time.sleep(0.1)
            