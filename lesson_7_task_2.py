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
    
    feed_users_val = part_of_data.feed_users.values[0]
    actions_val = part_of_data.actions.values[0]
    users_sent_val = part_of_data.users_sent.values[0]
    sent_messages_val = part_of_data.sent_messages.values[0]
    users_recieved_val = part_of_data.users_recieved.values[0]
    active_users_val = part_of_data.active_users.values[0]
    
    
    first_date_str = lst_dates[0].strftime("%Y-%m-%d")
    prev_date_str = prev_date.strftime("%Y-%m-%d")
    
    txt_msg = f"""Отчет по приложению(упражнение 2):
    
    Отчет за вчерашний день({prev_date_str}):
    
    Количество пользователей сервиса ленты: {feed_users_val}
    Количество пользователей сервиса сообщений 
    (которые посылают сообщения): {users_sent_val}
    Количество пользователей, которые получили 
    сообщения: {users_recieved_val}
    Количество пользователей сервиса сообщений 
    (которые посылают сообщения) и ленты: {active_users_val}
    Количество действий в сервисе ленты: {actions_val}
    Количество посланных сообщений: {sent_messages_val}
    
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
    
    fig, axs = plt.subplots(5, 1, figsize=(12, 20))
    plt.subplots_adjust(hspace=0.5)
    
    # 1 График
    tmp_df_1 = inp_df[["date", "feed_users"]].copy()
    tmp_df_1 = tmp_df_1.rename(columns={"feed_users": "users"})
    tmp_df_1["type_users"] = "feed_users"
    tmp_df_2 = inp_df[["date", "users_sent"]].copy()
    tmp_df_2 = tmp_df_2.rename(columns={"users_sent": "users"})
    tmp_df_2["type_users"] = "message_users"
    
    tmp_df = pd.concat([tmp_df_1, tmp_df_2])
    
    ax1 = sns.lineplot(data=tmp_df, x="date", y="users", 
                                    ax=axs[0], hue="type_users")
    ax1.axes.set_title("DAU feed/message", fontsize=20)    
    
    # 2 График
    ax2 = sns.lineplot(data=inp_df, x="date", y="active_users", 
                                    color="red", ax=axs[1])
    ax2.axes.set_title("DAU feed & message", fontsize=20)     
    
    # 3 График
    tmp_df_1 = inp_df[["date", "actions"]].copy()
    tmp_df_1["type_action"] = "feed_actions"
    tmp_df_2 = inp_df[["date", "sent_messages"]].copy()
    tmp_df_2 = tmp_df_2.rename(columns={"sent_messages": "actions"})
    tmp_df_2["type_action"] = "message_actions"
    
    tmp_df = pd.concat([tmp_df_1, tmp_df_2])
    
    ax3 = sns.lineplot(data=tmp_df, x="date", y="actions", 
                                    ax=axs[2], hue="type_action")
    ax3.axes.set_title("Daily actions", fontsize=20)
    
    # 4 График
    tmp_df = inp_df.copy()
    tmp_df = tmp_df.rename(columns={"users_recieved": "users"})
    
    ax4 = sns.lineplot(data=tmp_df, x="date", y="users", 
                                    color="red", ax=axs[3])
    ax4.axes.set_title("Daily recieved messages users", fontsize=20)
    
    # 5 График
    tmp_df["ratio_of_users"] = tmp_df["users"] / tmp_df["users_sent"]
    
    ax5 = sns.lineplot(data=tmp_df, x="date", y="ratio_of_users", 
                                    color="red", ax=axs[4])
    ax5.axes.set_title("Daily ratio of recieved and sent messages users", fontsize=20)    
    
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
def n_sitenkov_lesson_7_task_2():

    @task
    def get_feed_table():
        query_feed = """
        SELECT toDate(time) AS date,
               COUNT(DISTINCT user_id) AS feed_users,
               COUNT(user_id) AS actions,
               countIf(action = 'view') AS views,
               countIf(action = 'like') AS likes,
               views / likes AS CTR
            FROM simulator_20221220.feed_actions
        WHERE date BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date
        """
        
        data = Getch(query_feed).df
        return data
    
    @task
    def get_message_table():
        query_message = """
        SELECT toDate(time) AS date,
               COUNT(DISTINCT user_id) AS users_sent,
               COUNT(user_id) AS sent_messages,
               COUNT(DISTINCT reciever_id) AS users_recieved
        FROM simulator_20221220.message_actions
        WHERE date BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date
        """
        
        data = Getch(query_message).df
        return data
    
    @task
    def get_active_table():
        query_active = """
        SELECT date,
               COUNT(user_id) AS active_users
        FROM
            (SELECT toDate(time) AS date,
                    user_id,
                    COUNT(DISTINCT user_id) AS users
             FROM simulator_20221220.message_actions
             GROUP BY user_id, date) AS t1 INNER JOIN
             (SELECT toDate(time) AS date,
                    user_id,
                    COUNT(DISTINCT user_id) AS users
             FROM simulator_20221220.feed_actions
             GROUP BY user_id, date) AS t2
             USING(user_id, date)
        WHERE date BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date
        """
        
        data = Getch(query_active).df
        return data
    
    @task
    def get_merged_data(f_df, m_df, a_df):
        
        res_df = f_df.merge(m_df, on="date", how="inner")
        res_df = res_df.merge(a_df, on="date", how="inner")
        
        return res_df

    @task
    def get_app_report(inp_merged_table):
        
        txt_msg = get_msg(inp_merged_table)
        graphics_msg = get_graphics(inp_merged_table)
        
        bot.sendMessage(chat_id=chat_id, text=txt_msg)
        
        graphics_object = io.BytesIO()
    
        graphics_msg.savefig(graphics_object)
        graphics_object.seek(0)
        graphics_object.name = "app_report_plots.png"
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=graphics_object)
        
    
    feed_table = get_feed_table()
    message_table = get_message_table()
    active_table = get_active_table()
    merged_df = get_merged_data(feed_table, message_table, active_table)
    get_app_report(merged_df)

n_sitenkov_lesson_7_task_2 = n_sitenkov_lesson_7_task_2()
