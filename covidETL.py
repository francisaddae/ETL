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
