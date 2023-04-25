# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(query="Select 1", 
              host="https://karpov_courses_clickhouse_db", 
              user="user_name",
              password="password"):
    """
    Функция для запросов
    """
    
    r = requests.post(host, 
                      data=query.encode("utf-8"), 
                      auth=(user, password), 
                      verify=False)
    result = pd.read_csv(StringIO(r.text), sep="\t")
    
    return result

def get_dimensions(inp_df, col):
    """
    Функция формирования полного датафрейма с dimensions
    """
    
    res_df = inp_df.groupby(["event_date", col]).agg({"views": "sum",
                                                      "likes": "sum",
                                                      "messages_sent": "sum",
                                                      "users_sent": "sum",
                                                      "messages_received": "sum",
                                                      "users_received": "sum"})
    res_df = res_df.reset_index()
    res_df["dimension"] = col
    res_df = res_df.rename(columns = {col : "dimension_value"})
    
    return res_df


# Параметры для дополнения таблицы в clickhouse
tst_con = {"host": "https://karpov_courses_clickhouse_db",
           "database": "db",
           "user": "user_name", 
           "password": "password"
           }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    "owner": "sitenkov_n",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 26),
}

# Интервал запуска DAG(каждый день в 06:00)
schedule_interval = "0 6 * * *"

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def n_sitenkov_lesson_6_task_1():

    @task()
    def extract_feed_actions():
        query_f = """SELECT user_id,
                            toDate(time) as event_date,
                            countIf(action = 'view') AS views,
                            countIf(action = 'like') AS likes,
                            if(gender = 1, 'male', 'female') AS gender,
                            age,
                            os
                    FROM simulator_20221220.feed_actions 
                    WHERE toDate(time) = today() - 1
                    GROUP BY user_id, event_date, gender, age, os
                    format TSVWithNames"""
        
        df_cube_feed = ch_get_df(query=query_f)
        
        return df_cube_feed
    
    @task()
    def extract_message_actions():
        query_m = """SELECT id_user as user_id,
                            event_date,
                            gender,
                            age,
                            os,
                            messages_sent,
                            messages_received,
                            users_sent,
                            users_received
                            FROM 
                                (SELECT user_id AS id_user,
                                        toDate(time) AS event_date,
                                        if(gender = 1, 'male', 'female') AS gender,
                                        COUNT(user_id) AS messages_sent,
                                        COUNT(DISTINCT reciever_id) AS users_received,
                                        age,
                                        os
                                 FROM simulator_20221220.message_actions
                                 GROUP BY id_user, event_date, gender, age, os
                                ) AS t1 FULL OUTER JOIN
                                (SELECT *
                                 FROM 
                                    (SELECT DISTINCT user_id AS id_user,
                                            if(gender = 1, 'male', 'female') AS gender,
                                            os,
                                            age
                                     FROM simulator_20221220.message_actions
                                    ) AS t2 INNER JOIN
                                    (SELECT reciever_id AS id_user,
                                            toDate(time) AS event_date,
                                            COUNT(user_id) AS messages_received,
                                            COUNT(DISTINCT user_id) AS users_sent
                                     FROM simulator_20221220.message_actions 
                                     GROUP BY event_date, id_user) AS t3 
                                     ON t2.id_user = t3.id_user
                                ) AS t4 USING(id_user, event_date, gender, age, os)
                            WHERE event_date = today() - 1
                    format TSVWithNames"""
        
        df_cube_message = ch_get_df(query=query_m)
        
        return df_cube_message
    
    @task
    def join_data(df_cube_feed, df_cube_message):
        
        df_merged_cube = df_cube_feed.merge(df_cube_message, 
                                            on=["user_id", "event_date", 
                                                "age", "os",  "gender"],
                                            how="outer")
        return df_merged_cube

    @task
    def os_dim(df_merged):
        
        res_os = get_dimensions(df_merged, "os")
        
        return res_os

    @task
    def gender_dim(df_merged):
        
        res_gender = get_dimensions(df_merged, "gender")
        
        return res_gender
    
    @task
    def age_dim(df_merged):
        
        res_age = get_dimensions(df_merged, "age")
        
        return res_age    
    
    @task
    def update_table(os_df, gender_df, age_df):
        
        res_df = pd.concat((os_df, gender_df, age_df))
        res_df = res_df.reindex(columns=["event_date", "dimension", 
                                         "dimension_value", "views", 
                                         "likes", "messages_received", 
                                         "messages_sent", "users_received", 
                                         "users_sent"])
        res_df = res_df.astype({"event_date" : "datetime64", 
                                "views": "int64", 
                                "likes": "int64", 
                                "messages_received": "int64", 
                                "messages_sent": "int64", 
                                "users_received": "int64", 
                                "users_sent": "int64"})
        
        update_query = """CREATE TABLE IF NOT EXISTS db.n_sitenkov_table
                          (event_date Date, 
                           dimension String,
                           dimension_value String,
                           views Int64,
                           likes Int64,
                           messages_received Int64,
                           messages_sent Int64,
                           users_received Int64,
                           users_sent Int64) 
                           ENGINE = MergeTree()
                           ORDER BY event_date"""
        
        ph.execute(query=update_query, connection=tst_con)
        ph.to_clickhouse(df=res_df, 
                         table="n_sitenkov_table", 
                         connection=tst_con, 
                         index=False)
        

    df_cube_f = extract_feed_actions()
    df_cube_m = extract_message_actions()
    merged_df = join_data(df_cube_f, df_cube_m)
    df_os = os_dim(merged_df)
    df_gender = gender_dim(merged_df)
    df_age = age_dim(merged_df)
    update_table(df_os, df_gender, df_age)

n_sitenkov_lesson_6_task_1 = n_sitenkov_lesson_6_task_1() 
