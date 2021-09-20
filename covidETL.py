import requests as r
import json as js
import pandas as pd
import psycopg2 as pg
from prefect import task, context

message = context.get('message')


def get_data():
    try:
        url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
        response = r.get(url)

        df = pd.DataFrame(js.loads(response))
        return df

    except  Exception as error:
        message.info(error)
        return False


def transfrom_data(extracted_data):
    try:
        df = extracted_data[:,[0,4]]
        message.info(df)
    except  Exception as error:
        message.info(error)
        return False