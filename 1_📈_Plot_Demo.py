# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 11:21:26 2025

@author: Agropilot-Project
"""

import streamlit as st
import time
import numpy as np

st.set_page_config(page_title="Пример График", page_icon="📈")

st.markdown("# Демонстрация возможностей: График")
st.sidebar.header("Демонстрация возможностей: График")
st.write(
    """Эта демонстрация иллюстрирует комбинацию построения графика и анимации с помощью
Streamlet. Мы генерируем набор случайных чисел в цикле в течение примерно 5 секунд. Наслаждайтесь!"""
)

progress_bar = st.sidebar.progress(0)
status_text = st.sidebar.empty()
last_rows = np.random.randn(1, 1)
chart = st.line_chart(last_rows)

for i in range(1, 101):
    new_rows = last_rows[-1, :] + np.random.randn(5, 1).cumsum(axis=0)
    status_text.text("Прогресс %i%%" % i)
    chart.add_rows(new_rows)
    progress_bar.progress(i)
    last_rows = new_rows
    time.sleep(0.05)

progress_bar.empty()

st.button("Перестроить")