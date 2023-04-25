# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse


# Класс, взятый из файла, который был показан в лекции 7 урока.
class Getch:
    def __init__(self, query, db='db'):
        self.connection = {
            'host': 'https://karpov_courses_clickhouse_db',
            'password': 'password',
            'user': 'user_name',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)


    
def get_msg(inp_df):
    """
    Получение метрик за предыдущий день в виде текста
    """
    
    lst_dates = sorted(inp_df.date)
    prev_date = lst_dates[-1]
    
    part_of_data = inp_df[inp_df.date == prev_date]
    
    users_val = part_of_data.users.values[0]
    views_val = part_of_data.views.values[0]
    likes_val = part_of_data.likes.values[0]
    ctr_val = part_of_data.CTR.values[0]
    
    first_date_str = lst_dates[0].strftime("%Y-%m-%d")
    prev_date_str = prev_date.strftime("%Y-%m-%d")
    
    txt_msg = f"""Отчет по ленте(упражнение 1):
    
    Отчет за вчерашний день({prev_date_str}):
    
    Количество пользователей: {users_val}
    Количество просмотров: {views_val}
    Количество лайков: {likes_val}
    CTR: {round(ctr_val, 3)}
    
    Графики за предыдущие 7 дней с {first_date_str} по {prev_date_str} 
    включительно по дням:
    
    """
    
    return txt_msg


def get_graphics(inp_df):
    """
    Получение метрик за последние 7 дней в виде графиков
    """
    
    sns.set(
    font_scale=1,
    style="darkgrid",
    rc={"figure.figsize": (10, 7)}
    )
    
    fig, axs = plt.subplots(4, 1, figsize=(12, 16))
    plt.subplots_adjust(hspace=0.5)
    
    ax1 = sns.lineplot(data=inp_df, x="date", y="users", 
                                  color="purple", ax=axs[0])
    ax1.axes.set_title("DAU", fontsize=20)
    
    ax2 = sns.lineplot(data=inp_df, x="date", y="views", 
                                  color="purple", ax=axs[1])
    ax2.axes.set_title("Daily views", fontsize=20)
    
    ax3 = sns.lineplot(data=inp_df, x="date", y="likes", 
                                  color="purple", ax=axs[2])
    ax3.axes.set_title("Daily likes", fontsize=20)
    
    ax4 = sns.lineplot(data=inp_df, x="date", y="CTR", 
                                  color="purple", ax=axs[3])
    ax4.axes.set_title("Daily CTR", fontsize=20)
    
    return fig

            

# Данные по телеграм боту        
my_token = "my_token"
bot = telegram.Bot(token=my_token)

chat_id = "-123456789"


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    "owner": "sitenkov_n",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 29),
}


# Интервал запуска DAG
schedule_interval = "0 11 * * *"

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def n_sitenkov_lesson_7_task_1():

    @task
    def get_feed_report():
    
        query_report = """
        SELECT toDate(time) AS date,
               COUNT(DISTINCT user_id) AS users,
               countIf(action = 'view') AS views,
               countIf(action = 'like') AS likes,
               likes / views AS CTR
            FROM simulator_20221220.feed_actions
        WHERE date BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date
        """
        
        data = Getch(query_report).df
        
        txt_msg = get_msg(data)
        graphics_msg = get_graphics(data)
        
        bot.sendMessage(chat_id=chat_id, text=txt_msg)
        
        graphics_object = io.BytesIO()
    
        graphics_msg.savefig(graphics_object)
        graphics_object.seek(0)
        graphics_object.name = "feed_report_plots.png"
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=graphics_object)


    get_feed_report()

n_sitenkov_lesson_7_task_1 = n_sitenkov_lesson_7_task_1()
