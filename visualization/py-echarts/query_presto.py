#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyhive import presto

PRESTO_SERVER = {'host': 'bigdata', 'port': 8080, 'catalog': 'hive', 'schema': 'default'}
CITY_PRICE_SUM_QUERY="select t2.city_name as cityName, sum(per_pay) as totalPay from user_pay t1 join shop_info t2 on t1.shop_id=t2.shop_id group by t2.city_name order by totalPay desc limit 10"

DAY_PAY_COUNT_QUERY="select substr(t1.time_stamp,1,10) as day, count(*) as countPay from user_pay t1 group by substr(t1.time_stamp,1,10) order by countPay desc limit 10"

DAY_VIEW_COUNT_QUERY="select substr(t1.time_stamp,1,10) as day, count(*) as countView from user_view t1 group by substr(t1.time_stamp,1,10) order by countView desc limit 10"

DAY_PAY_AND_VIEW_COUNT_QUERY="select tpay.day as day, countPay, CASE WHEN countView is null THEN 0 ELSE countView END as countView from (select substr(t1.time_stamp,1,10) as day, count(*) as countPay from user_pay t1 group by substr(t1.time_stamp,1,10)) as tpay left join (select substr(t1.time_stamp,1,10) as day, count(*) as countView from user_view t1 group by substr(t1.time_stamp,1,10)) as tview on tpay.day = tview.day order by day desc limit 10"


DAY_PAY_COUNT_QUERYALL="select substr(t1.time_stamp,1,10) as day, count(*) as countPay from user_pay t1 group by substr(t1.time_stamp,1,10) order by day"

DAY_VIEW_COUNT_QUERYALL="select substr(t1.time_stamp,1,10) as day, count(*) as countView from user_view t1 group by substr(t1.time_stamp,1,10) order by day"

DAY_PAY_AND_VIEW_COUNT_QUERYALL="select tpay.day as day, countPay, CASE WHEN countView is null THEN 0 ELSE countView END as countView from (select substr(t1.time_stamp,1,10) as day, count(*) as countPay from user_pay t1 group by substr(t1.time_stamp,1,10)) as tpay left join (select substr(t1.time_stamp,1,10) as day, count(*) as countView from user_view t1 group by substr(t1.time_stamp,1,10)) as tview on tpay.day = tview.day order by day desc limit 10"

class Presto_Query:

    def query_city_sum_price(self):
        conn = presto.connect(**PRESTO_SERVER)
        cursor = conn.cursor()
        cursor.execute(CITY_PRICE_SUM_QUERY)
        tuples=cursor.fetchall()
        return tuples

    def query_day_count_pay(self):
        conn = presto.connect(**PRESTO_SERVER)
        cursor = conn.cursor()
        cursor.execute(DAY_PAY_COUNT_QUERY)
        tuples=cursor.fetchall()
        return tuples

    def query_day_count_view(self):
        conn = presto.connect(**PRESTO_SERVER)
        cursor = conn.cursor()
        cursor.execute(DAY_VIEW_COUNT_QUERY)
        tuples=cursor.fetchall()
        return tuples

    def query_day_count_pay_and_view(self):
        conn = presto.connect(**PRESTO_SERVER)
        cursor = conn.cursor()
        cursor.execute(DAY_PAY_AND_VIEW_COUNT_QUERY)
        tuples=cursor.fetchall()
        return tuples

    def query_day_count_payALL(self):
        conn = presto.connect(**PRESTO_SERVER)
        cursor = conn.cursor()
        cursor.execute(DAY_PAY_COUNT_QUERYALL)
        tuples=cursor.fetchall()
        return tuples

    def query_day_count_viewALL(self):
        conn = presto.connect(**PRESTO_SERVER)
        cursor = conn.cursor()
        cursor.execute(DAY_VIEW_COUNT_QUERYALL)
        tuples=cursor.fetchall()
        return tuples

    def query_day_count_pay_and_viewALL(self):
        conn = presto.connect(**PRESTO_SERVER)
        cursor = conn.cursor()
        cursor.execute(DAY_PAY_AND_VIEW_COUNT_QUERYALL)
        tuples=cursor.fetchall()
        return tuples


    def getKeys(self,tuples):
        keys=[]
        for tuple in tuples:
            keys.append(tuple[0])
        return keys

    def getValues(self, tuples):
        values=[]
        for tuple in tuples:
            values.append(tuple[1])
        return values

    def getValues2(self, tuples):
        values=[]
        for tuple in tuples:
            values.append(tuple[2])
        return values

    def getCityPriceDict(self, tuples):
        dict={}
        i = 0
        for tuple in tuples:
            i = i+1
            city=tuple[0]
            price=long(tuple[1])
            dict["top"+bytes(i)+":"+city]=price
        return dict
