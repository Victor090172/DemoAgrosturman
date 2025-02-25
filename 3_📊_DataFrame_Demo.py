# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 12:13:55 2025

@author: Agropilot-Project
"""

import streamlit as st
import pandas as pd
import altair as alt
from urllib.error import URLError

st.set_page_config(page_title="Пример Набор данных", page_icon="📊")

st.markdown("# Демонстрация возможностей: Набор данных")
st.sidebar.header("Демонстрация возможностей: Набор данных")
st.write(
    """В этой демонстрации показано, как использовать "st.write" для визуализации фреймов данных Pandas.
(Данные предоставлены [UN Data Explorer](http://data.un.org/Explorer.aspx).)"""
)

@st.cache_data
def get_UN_data():
    AWS_BUCKET_URL = "http://streamlit-demo-data.s3-us-west-2.amazonaws.com"
    df = pd.read_csv(AWS_BUCKET_URL + "/agri.csv.gz")
    return df.set_index("Region")


try:
    df = get_UN_data()
    countries = st.multiselect(
        "Выберите страну", list(df.index), ["China", "United States of America"]
    )
    if not countries:
        st.error("Пожалуйста, выберите хотя бы одну страну.")
    else:
        data = df.loc[countries]
        data /= 1000000.0
        st.write("### Валовое сельскохозяйственное производство ($B)", data.sort_index())

        data = data.T.reset_index()
        data = pd.melt(data, id_vars=["index"]).rename(
            columns={"index": "Год", "value": "Валовое с/х производство ($B)"}
        )
        chart = (
            alt.Chart(data)
            .mark_area(opacity=0.3)
            .encode(
                x="Год:T",
                y=alt.Y("Валовое с/х производство ($B):Q", stack=None),
                color="Region:N",
            )
        )
        st.altair_chart(chart, use_container_width=True)
except URLError as e:
    st.error(
        """
        **Для этой демонстрации требуется доступ в Интернет.**
        Ошибка соединения: %s
    """
        % e.reason
    )
