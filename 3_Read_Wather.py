# -*- coding: utf-8 -*-
"""
Created on Tue Apr  1 12:24:50 2025

@author: Agropilot-Project
"""

import streamlit as st
import httpx
import pandas as pd
import json
import requests
import openmeteo_requests
import requests_cache
from retry_requests import retry
import psycopg2
import datetime
import time
from io import StringIO
pd.options.display.max_columns = None