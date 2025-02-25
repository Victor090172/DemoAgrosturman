# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 11:42:58 2025

@author: Agropilot-Project
"""
import streamlit as st

pages = {
    "Демостенд": [
        st.Page("about.py", title="О проекте"),
        st.Page("1_Import_Data.py", title="Импорт данных"),
    ],
    "Примеры": [
        st.Page("1_📈_Plot_Demo.py", title="Демо График"),
        st.Page("2_🌍_Map_Demo.py", title="Демо Карта"),
        st.Page("3_📊_DataFrame_Demo.py", title="Демо Данные"),
    ],
}

pg = st.navigation(pages)
pg.run()