# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 11:42:58 2025

@author: Agropilot-Project
"""
import streamlit as st


def nav_to(url: str):
    nav_script = """
        <meta http-equiv="refresh" content="0; url='%s'">
    """ % (
        url
    )
    st.write(nav_script, unsafe_allow_html=True)
    st.stop()

def itsm_page():
    nav_to(url="https://datalens.yandex/c5n3piuo02xyz")

def bi_page():
    nav_to(url="https://datalens.yandex/hl5lwcqffoji3")

st.logo("images/Агропилот.png")

pages = {
    "Демостенд": [
        st.Page("about.py", title="О проекте", icon= '🏠'),
        st.Page("1_Import_Data.py", title="Импорт объектов", icon="🚛"),
        st.Page("2_Import_Geozone.py", title="Импорт геозон", icon='🗺️'),
        st.Page("4_Import_zone_works.py", title="Импорт работ в геозонах", icon="🛠️"),
        st.Page("3_Read_Wather.py", title="Прогноз погоды", icon="⛅"),
    ],
    "Аналитика": [
        st.Page(bi_page, title ="Анализ данных Форт Монитор"),
        st.Page(itsm_page, title ="Анализ данных ITSM365"),
    ],
    "Примеры": [
        st.Page("1_📈_Plot_Demo.py", title="Демо График"),
        st.Page("2_🌍_Map_Demo.py", title="Демо Карта"),
        st.Page("3_📊_DataFrame_Demo.py", title="Демо Данные"),
    ],
}

pg = st.navigation(pages)
pg.run()
