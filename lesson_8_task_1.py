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


# Для удобства построения графиков в запрос можно добавить колонки date, hm
# Для запросов по feed_actions
query_tmp_feed = """
SELECT toStartOfFifteenMinutes(time) as ts,
       toDate(ts) as date,
       formatDateTime(ts, '%R') as hm,
       uniqExact(user_id) as number_of_users_lenta,
       COUNT(user_id) AS number_of_actions_lenta,
       countIf(action = 'view') AS number_of_views,
       countIf(action = 'like') AS number_of_likes,
       number_of_likes / number_of_views AS CTR,
       #group_replace
FROM simulator_20221220.feed_actions
WHERE ts >=  today() - 30 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm, #group_replace
ORDER BY #group_replace, ts"""

# Для запросов по message_actions
query_tmp_message = """
SELECT toStartOfFifteenMinutes(time) as ts,
       toDate(ts) as date,
       formatDateTime(ts, '%R') as hm,
       uniqExact(user_id) as number_of_users_messages,
       COUNT(user_id) AS number_of_actions_message,
       #group_replace
FROM simulator_20221220.message_actions
WHERE ts >=  today() - 30 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm, #group_replace
ORDER BY #group_replace, ts"""

# Словарь для названий графиков
map_metrics = {
    "number_of_users_lenta": "Graph of the number of users over time",
    "number_of_actions_lenta": "Graph of the number of actions over time",
    "number_of_views": "Graph of the number of views over time",
    "number_of_likes": "Graph of the number of likes over time",
    "CTR": "Graph of the CTR over time",
    "number_of_users_messages": "Graph of the number of users over time",
    "number_of_actions_message": "Graph of the number of actions over time"
    }


# Данные по телеграм боту
my_token = "my_token"
bot = telegram.Bot(token=my_token)
chat_id = -123456789


def get_borders(x):
    """
    Получение границ для алертов
    """
    
    arr = x.values
    
    q1 = np.quantile(arr, 0.25)
    q3 = np.quantile(arr, 0.75)
    lower_border = q1 - 1.5 * (q3 - q1)
    upper_border = q3 + 1.5 * (q3 - q1)
    median_val = np.median(arr)
    
    if lower_border < 0:
        lower_border = 0
    
    return lower_border, upper_border, median_val


def get_prep_df(df, metric, gr, gr_val):
    """
    Преобразование таблицы для анализа возможного алерта
    """
    
    df = df[df[gr] == gr_val].copy()

    df_curr = df[df.date == df.date.max()].copy()
    df_curr["type_df"] = "current"
    max_time = df_curr.hm.max()
    
    df_prev = df[df.date != df.date.max()].copy()
    df_prev = df_prev[df_prev.hm <= max_time].copy()
    df_prev["type_df"] = "previous"
    
    df_prev_ag = df_prev.groupby(["hm", "type_df"], 
                          as_index=False).agg({metric: get_borders})
    
    df_prev_ag["lower_borders"] = df_prev_ag[metric].apply(lambda x: x[0])
    df_prev_ag["upper_borders"] = df_prev_ag[metric].apply(lambda x: x[1])
    df_prev_ag["median_vals"] = df_prev_ag[metric].apply(lambda x: x[2])
    
    df_1 = df_prev_ag[["hm", "lower_borders"]]
    df_1 = df_1.rename(columns={"lower_borders": metric})
    df_1["type_values"] = "lower_borders"
    
    df_2 = df_prev_ag[["hm", "upper_borders"]]
    df_2 = df_2.rename(columns={"upper_borders": metric})
    df_2["type_values"] = "upper_borders"
    
    df_3 = df_prev_ag[["hm", "median_vals"]]
    df_3 = df_3.rename(columns={"median_vals": metric})
    df_3["type_values"] = "median_vals"
    
    df_curr["type_values"] = "current_values"
    
    res_dt = pd.concat([df_1, df_2, df_3, 
                        df_curr[["hm", metric, "type_values"]]])
    
    return res_dt.reset_index(drop=True)


def check_anomaly(df, metric):
    """
    Проверка на наличие аномалии
    """
    
    df = df[df.hm == df.hm.max()].copy()
    
    lower_border = df[df["type_values"] == "lower_borders"][metric].values[0]
    upper_border = df[df["type_values"] == "upper_borders"][metric].values[0]
    median_val = df[df["type_values"] == "median_vals"][metric].values[0]
    current_value = df[df["type_values"] == "current_values"][metric].values[0]
    
    # вычисляем отклонение
    try:
        diff = abs(current_value - median_val) / median_val
    except:
        diff = -1
    
    # Проверка на alert
    if lower_border >= current_value or upper_border <= current_value:
        is_alert = 1
    else:
        is_alert = 0           
        
    return is_alert, current_value, diff


def make_report(data, current_value, diff, gr_name, gr_val, metric_name):
    """
    Формирование описания алерта
    """        
    data = data[data["type_values"] != "median_vals"].copy()
    max_time = data.hm.max()
    
    if diff != -1:
        msg = f"""Метрика {metric_name} в срезе {gr_name} со значением среза {gr_val}.
Текущее значение {current_value:.2f}.
Отклонение более {diff:.2%} по сравнению с медианным значением на {max_time} за последние 30 дней"""
    else:
         msg = f"""Метрика {metric_name} в срезе {gr_name} со значением среза {gr_val}.
Текущее значение {current_value:.2f}.
Не вышло вычислить отклонение по сравнению с медианным значением на {max_time} за последние 30 дней"""
    
    # задаем размер графика
    sns.set(rc={"figure.figsize": (16, 10)}) 
    plt.tight_layout()
    
    # строим линейный график
    ax = sns.lineplot( 
        # задаем датафрейм для графика
        data=data[data["type_values"] != "median_vals"],
        # указываем названия колонок в датафрейме для x и y
        x="hm", 
        y=metric_name, 
        # задаем "группировку" на графике, т е хотим чтобы для
        # каждого значения date была своя линия построена
        hue="type_values" 
        )
    
    # этот цикл нужен чтобы разрядить подписи координат по оси Х
    for ind, label in enumerate(ax.get_xticklabels()): 
        if ind % 8 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)
    # задаем имя оси Х
    ax.set(xlabel="time")
    # задаем имя оси У
    ax.set(ylabel=metric_name) 
    
    # задаем заголовок графика
    ax.set_title("{}".format(map_metrics[metric_name]))
    # задаем лимит для оси У
    ax.set(ylim=(0, None)) 

    # формируем файловый объект
    plot_object = io.BytesIO()
    ax.figure.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = "{0}.png".format(metric_name)
    plt.close()

    # отправляем алерт
    bot.sendMessage(chat_id=chat_id, text=msg)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)    


def make_results(df, metrics, gr_name, group_vals):
    """
    Проведение анализа и формирование возможного описания алерта
    """     
    for group_val in group_vals:
        for metric in metrics:
            prep_df = get_prep_df(df, metric, 
                                  gr_name, group_val)
            
            is_alert, current_value, diff = check_anomaly(prep_df, metric)
            if is_alert:
                make_report(prep_df, current_value, diff, gr_name, 
                            group_val, metric)
                

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    "owner": "sitenkov_n",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 31),
}


# Интервал запуска DAG
schedule_interval = "*/15 * * * *"

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def n_sitenkov_lesson_8_task_1():

    @task
    def get_os_feed_data():
        """
        Получение преобразованной таблицы feed_actions по группе os
        """
        
        query = query_tmp_feed.replace("#group_replace", "os")
        data = Getch(query).df
        
        return data
    
    
    @task
    def get_os_message_data():
        """
        Получение преобразованной таблицы message_actions по группе os
        """    
        
        query = query_tmp_message.replace("#group_replace", "os")
        data = Getch(query).df
        
        return data
    
    
    @task
    def get_source_feed_data():
        """
        Получение преобразованной таблицы feed_actions по группе source
        """   
        
        query = query_tmp_feed.replace("#group_replace", "source")
        data = Getch(query).df
        
        return data
    
    
    @task
    def get_source_message_data():
        """
        Получение преобразованной таблицы message_actions по группе source
        """    
        
        query = query_tmp_message.replace("#group_replace", "source")
        data = Getch(query).df
        
        return data
    
    
    @task
    def run_alerts(os_feed_df, os_message_df,
                   source_feed_df, source_message_df):
        """
        Функция по обнаружению алертов 
        """
        
        feed_metrics = ["number_of_users_lenta", 
                        "number_of_actions_lenta", 
                        "number_of_views", 
                        "number_of_likes", 
                        "CTR"]
        msg_metrics = ["number_of_users_messages", 
                       "number_of_actions_message"]    
        
        group_vals_os = ["Android", "iOS"]
        group_vals_source = ["ads", "organic"]
        
        # os_feed_df
        make_results(os_feed_df, feed_metrics, "os", group_vals_os)
        # source_feed_df
        make_results(source_feed_df, feed_metrics, "source", group_vals_source)
        # os_message_df
        make_results(os_message_df, msg_metrics, "os", group_vals_os)
        # source_message_df
        make_results(source_message_df, msg_metrics, "source", group_vals_source)
    
    
    os_feed_df = get_os_feed_data()
    os_message_df = get_os_message_data()
    source_feed_df = get_source_feed_data()
    source_message_df = get_source_message_data()
    run_alerts(os_feed_df, os_message_df,
               source_feed_df, source_message_df)    
    

n_sitenkov_lesson_8_task_1 = n_sitenkov_lesson_8_task_1()
