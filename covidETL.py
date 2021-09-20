'''
  This etl process is a copy of a known pipline written by Jonathan Duran.
  His was in airflow. This showcases how to replicate that in prefect pipeline.
  https://python.plainenglish.io/simple-etl-with-airflow-372b0109549
'''



from io import StringIO
import requests as r
import json as js
import pandas as pd
import psycopg2 as pg
import datetime as dt
from prefect import task, context
from requests.api import request

logger = context.get('logger')


def get_data():
    try:
        url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
        response = r.get(url)

        df = pd.DataFrame(js.loads(response.content))
        df.to_csv(f"nycCovid_{dt.date.today().strftime('%Y%m%d')}.csv",
                header=True, index=False)
        return df

    except  Exception as error:
        logger.info(error)
        return False


def transfrom_data(extracted_data):
    try:
        df = extracted_data[['date_of_interest', 'case_count', 'probable_case_count','hospitalized_count']]
        df['date_of_interest'] = pd.to_datetime(df['date_of_interest'], infer_datetime_format=True, errors='raise')
        df['date_of_interest'] = df['date_of_interest'].dt.date
        df[['case_count', 'probable_case_count','hospitalized_count']] = df[['case_count', 'probable_case_count','hospitalized_count']].astype(int)
        logger.info(f'\n{df.head(5)}')

        return df

    except  Exception as error:
        logger.info(error)
        return False

def load_data(transformed_data):
    try:
        conn = pg.connect(
            database='maindb',
            user='postgres',
            password=str(input('Enter the password for postgres: ')),
            host='localhost',port=5432
        )
        request = conn.cursor()
        schema = '''
                    CREATE TABLE IF NOT EXISTS covid_data(
                        date DATE,
                        case_count INT,
                        hospitalized_count INT,
                        death_count INT,
                        PRIMARY KEY (date)
                    );
                    TRUNCATE TABLE covid_data;
                 '''
        request.execute(schema)

        buffer = StringIO()
        transformed_data.to_csv(buffer, header=False, index=False)
        buffer.seek(0)
        request.copy_from(buffer, 'covid_data', sep=',')

    except  Exception as error:
        logger.info(error)
        return False

    finally:
        if True:
            conn.commit()
            request.close()
            return conn
