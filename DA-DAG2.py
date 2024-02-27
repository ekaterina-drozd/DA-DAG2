import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

year = 1994 + hash(f'ek-drozd') % 23
vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'ek-drozd',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 11),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup = False)
def ek_drozd_DAG_3():
    #получаем данные
    @task(retries=3, retry_delay=timedelta(5))
    def get_data():
        sales = pd.read_csv(vgsales).query('Year == @year')
        return sales
        
    #1)Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3, retry_delay=timedelta(5))
    def most_global_sales_name(sales):
        most_global_sales = sales.groupby('Name', as_index = False)\
                                    .agg({'Global_Sales': 'sum'})\
                                    .query('Global_Sales == Global_Sales.max()')\
                                    .head(1).Name.values[0]
        return most_global_sales

    #2)Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=3, retry_delay=timedelta(5))
    def most_eu_sales_genre(sales):
        most_eu_sales = sales.groupby('Genre', as_index = False)\
                                .agg({'EU_Sales': 'sum'})\
                                .query('EU_Sales == EU_Sales.max()').Genre.values[0]
        return most_eu_sales

    #3)На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько 
    @task(retries=3, retry_delay=timedelta(5))
    def most_na_sales_platform(sales):
        most_na_sales = sales.query('NA_Sales > 1')\
                                        .groupby('Platform', as_index = False)\
                                        .agg({'Name': 'nunique'})\
                                        .query('Name == Name.max()').Platform.values[0]
        return most_na_sales
    
    #4)У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task(retries=3, retry_delay=timedelta(5))
    def most_jp_sales_publisher(sales):
        most_jp_sales = sales.groupby('Publisher', as_index = False)\
                                        .agg({'JP_Sales': 'sum'})\
                                        .query('JP_Sales == JP_Sales.max()').Publisher.values[0]
        return most_jp_sales
    
    #5)Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=3, retry_delay=timedelta(5))
    def eu_jp_sales (sales):
        eu_jp = sales.query('EU_Sales > JP_Sales')\
                        .Name.nunique()
        return eu_jp

    #вывод
    @task(retries=3, retry_delay=timedelta(5), on_success_callback=send_message)
    def print_data(most_global_sales, most_eu_sales, most_na_sales, most_jp_sales, eu_jp):

        context = get_current_context()
        date = context['ds']
        print(f'''Дата: {date}

                  1)Какая игра была самой продаваемой в этом году во всем мире в {year} годy? - {most_global_sales}
                  
                  2)Игры какого жанра были самыми продаваемыми в Европе в {year} году? - {most_eu_sales}
                  
                  3)На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {year} году? - {most_na_sales}
                  
                  4)У какого издателя самые высокие средние продажи в Японии в {year} году? - {most_jp_sales}
                  
                  5)Сколько игр продались лучше в Европе, чем в Японии в {year} году? - {eu_jp}''')

        
    sales = get_data()
    most_global_sales = most_global_sales_name(sales)
    most_eu_sales = most_eu_sales_genre(sales)
    most_na_sales = most_na_sales_platform(sales)
    most_jp_sales = most_jp_sales_publisher(sales)
    eu_jp = eu_jp_sales (sales)
    
    print_data(most_global_sales, most_eu_sales, most_na_sales, most_jp_sales, eu_jp)

ek_drozd_DAG_3 = ek_drozd_DAG_3()
