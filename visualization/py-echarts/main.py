# -*- coding:utf-8 -*-

from flask import Flask, render_template
import json
from models import Chart
from query_presto import Presto_Query
from query_redis import Redis_Query

app = Flask(__name__)

@app.route("/")
def index():
    render = {
        "title": u"阿里巴巴口碑商家客流量分析系统",
        "templates": [
        ]
    }
    return render_template("index.html", **render)

@app.route("/city")
def city():
    presto=Presto_Query()

    city_price_tuples=presto.query_city_sum_price()
    city_price_dict=presto.getCityPriceDict(city_price_tuples)
    chartCity11 = Chart().pie("饼图", data=city_price_dict)

    keys=presto.getKeys(city_price_tuples)
    values=presto.getValues(city_price_tuples)
    chartCity12 = Chart() \
        .x_axis(data=keys) \
        .y_axis(formatter="{value}") \
        .bar(u"City Price", values, show_item_label=True)

    render = {
        "title": u"按城市统计交易金额排行",
        "templates": [
            {"type": "chart", "title":u"交易量Top10城市(饼状)", "option": json.dumps(chartCity11, indent=2)},
            {"type": "chart", "title":u"交易量Top10城市(柱状)", "option": json.dumps(chartCity12, indent=2)}
        ]
    }
    return render_template("main.html", **render)


@app.route("/day")
def day():
    presto=Presto_Query()

    day_count_pay_tuples=presto.query_day_count_pay()
    keys=presto.getKeys(day_count_pay_tuples)
    values=presto.getValues(day_count_pay_tuples)
    chartDayCountPay= Chart() \
        .x_axis(data=keys) \
        .y_axis(formatter="{value}") \
        .bar(u"Pay Count Per Day", values, show_item_label=True)


    day_count_view_tuples=presto.query_day_count_view()
    keys2=presto.getKeys(day_count_view_tuples)
    values2=presto.getValues(day_count_view_tuples)
    chartDayCountView= Chart() \
        .x_axis(data=keys2) \
        .y_axis(formatter="{value}") \
        .bar(u"View Count Per Day", values2, show_item_label=True)

    #day_count_pay_and_view_tuples=presto.query_day_count_pay_and_view()
    #keys3=presto.getKeys(day_count_pay_and_view_tuples)
    #values3=presto.getValues(day_count_pay_and_view_tuples)
    #values3_2=presto.getValues2(day_count_pay_and_view_tuples)
    #chartDayCountPayAndView= Chart() \
    #    .x_axis(data=keys3) \
    #    .y_axis(formatter="{value}") \
    #    .line(u"Pay Count and View Count of Last 10 Day", values3, show_item_label=True)

    day_count_pay_and_viewALL_tuples=presto.query_day_count_pay_and_viewALL()

    keys4=presto.getKeys(day_count_pay_and_viewALL_tuples)
    values41=presto.getValues(day_count_pay_and_viewALL_tuples)
    chartDayCountPayALL= Chart() \
        .x_axis(data=keys4) \
        .y_axis(formatter="{value}") \
        .line(u"Pay Count of Per Day", values41, show_item_label=True)

    values42=presto.getValues2(day_count_pay_and_viewALL_tuples)
    chartDayCountViewALL= Chart() \
        .x_axis(data=keys4) \
        .y_axis(formatter="{value}") \
        .line(u"View Count of Per Day", values42, show_item_label=True)

    render = {
        "title": u"按日期统计交易笔数和浏览次数排行",
        "templates": [
            {"type": "chart", "title":u"交易笔数Top10日期(柱状)", "option": json.dumps(chartDayCountPay, indent=2)},
            {"type": "chart", "title":u"浏览次数Top10日期(柱状)", "option": json.dumps(chartDayCountView, indent=2)},
            #{"type": "chart", "title":u"最近10日的交易笔数和浏览笔数(曲线)", "option": json.dumps(chartDayCountPayAndView, indent=2)},
            {"type": "chart", "title":u"每日交易笔数(曲线)", "option": json.dumps(chartDayCountPayALL, indent=2)},
            {"type": "chart", "title":u"每日浏览次数(曲线)", "option": json.dumps(chartDayCountViewALL, indent=2)}
        ]
    }
    return render_template("main.html", **render)


if __name__ == "__main__":
    app.run(debug=True,host="0.0.0.0",port=5001)