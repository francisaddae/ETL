import covidETL
from prefect import Flow

if __name__ == '__main__':

    with Flow('daily Covid') as dc:
        a = covidETL.get_data()
        b = covidETL.transfrom_data(a)
        c = covidETL.load_data(b)
    dc.run()