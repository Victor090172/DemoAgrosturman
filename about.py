# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 10:45:49 2025

@author: Agropilot-Project
"""

import streamlit as st

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.write("# Приветствуем на ДЕМО-стенде ГК АГРОштурман! 👋")

st.sidebar.success("О стенде")

st.markdown(
    """
    Данный стенд разработан для отработки технологии переноса данных из систем мониторинга, а также для демонстрации
    некоторых возможностей по оторбражению данных вне контура BI.
    ### Вы можете:
    - Посетить наш сайт [agrosturman.ru](https://agrosturman.ru/)
    - Почитать документацию по используемой технологии [documentation](https://docs.streamlit.io)
    - Направить свои идеи и пожелания [на почту](mailto:rvl@agropilot.ru)
"""
)