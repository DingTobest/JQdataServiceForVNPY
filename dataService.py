# encoding: UTF-8

from __future__ import print_function
import sys
import json
from datetime import datetime
from time import time, sleep

from pymongo import MongoClient, ASCENDING

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME, DAILY_DB_NAME

import jqdatasdk

# 加载配置
config = open('config.json')
setting = json.load(config)

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']
JQDATA_USER = setting['JQDATA_USER']
JQDATA_PASSWORD = setting['JQDATA_PASSWORD']

mc = MongoClient(MONGO_HOST, MONGO_PORT)        # Mongo连接
minute_db = mc[MINUTE_DB_NAME]                         # 分钟数据库
daily_db = mc[DAILY_DB_NAME]                    # 日线数据库

#----------------------------------------------------------------------
# 生成日Bar
def generateDailyVtBar(symbol, date, d):
    """生成K线"""
    bar = VtBarData()
    bar.vtSymbol = symbol
    bar.symbol = symbol
    bar.open = float(d['open'])
    bar.high = float(d['high'])
    bar.low = float(d['low'])
    bar.close = float(d['close'])
    bar.date = datetime.strptime(date[0:10], '%Y-%m-%d').strftime('%Y%m%d')
    bar.time = ''
    bar.datetime = datetime.strptime(bar.date, '%Y%m%d')
    bar.volume = d['volume']

    return bar

#----------------------------------------------------------------------
# 生成分钟Bar
def generateVtBar(symbol, time, d):
    """生成K线"""
    bar = VtBarData()
    bar.vtSymbol = symbol
    bar.symbol = symbol
    bar.open = float(d['open'])
    bar.high = float(d['high'])
    bar.low = float(d['low'])
    bar.close = float(d['close'])
    bar.date = datetime.strptime(time[0:10], '%Y-%m-%d').strftime('%Y%m%d')
    bar.time = time[11:]
    bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
    bar.volume = d['volume']
    
    return bar

#----------------------------------------------------------------------
# 单合约日线下载并保存到mongodb
def downDailyBarBySymbol(symbol, info, trade_date):
    start = time()

    symbol_name = info['name']
    cl = daily_db[symbol_name]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)  # 添加索引

    # 在此时间段内可以获取期货夜盘数据
    daily_df = jqdatasdk.get_price(symbol, start_date=trade_date, end_date=trade_date, frequency='daily')

    # 将数据传入到数据队列当中
    for index, row in daily_df.iterrows():
        bar = generateDailyVtBar(symbol_name, str(index), row)
        d = bar.__dict__
        flt = {'datetime': bar.datetime}
        cl.replace_one(flt, d, True)

    e = time()
    cost = (e - start) * 1000

    print(u'合约%s日线数据下载完成%s，耗时%s毫秒' % (symbol_name, trade_date, cost))

#----------------------------------------------------------------------
# 单合约分钟线下载并保存到mongodb
def downMinuteBarBySymbol(symbol, info, today, pre_trade_day):
    start = time()

    symbol_name = info['name']
    cl = minute_db[symbol_name]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)  # 添加索引

    # 在此时间段内可以获取期货夜盘数据
    minute_df = jqdatasdk.get_price(symbol, start_date=pre_trade_day + " 20:30:00",end_date=today + " 20:30:00", frequency='minute')

    # 将数据传入到数据队列当中
    for index, row in minute_df.iterrows():
        bar = generateVtBar(symbol_name, str(index), row)
        d = bar.__dict__
        flt = {'datetime': bar.datetime}
        cl.replace_one(flt, d, True)

    e = time()
    cost = (e - start) * 1000

    print(u'合约%s数据下载完成%s - %s，耗时%s毫秒' % (symbol_name, pre_trade_day, today, cost))

#----------------------------------------------------------------------
# 当日数据下载，定时任务使用
def downloadAllMinuteBar():
    jqdatasdk.auth(JQDATA_USER, JQDATA_PASSWORD)
    """下载所有配置中的合约的分钟线数据"""
    print('-' * 50)
    print(u'开始下载合约分钟线数据')
    print('-' * 50)

    today = datetime.today().date()

    trade_date_list = jqdatasdk.get_trade_days(end_date=today, count=2)

    symbols_df = jqdatasdk.get_all_securities(types=['futures'], date=today)
    
    for index, row in symbols_df.iterrows():
        downMinuteBarBySymbol(index, row, str(today), str(trade_date_list[-2]))

    print('-' * 50)
    print(u'合约分钟线数据下载完成')
    print('-' * 50)
    return

#----------------------------------------------------------------------
# 按日期一次性补全数据
def downloadBarByDate(start_date, end_date=datetime.today().date()):
    jqdatasdk.auth(JQDATA_USER, JQDATA_PASSWORD)
    """下载所有配置中的合约的分钟线数据"""
    print('-' * 50)
    print(u'开始下载合约分钟线数据')
    print('-' * 50)

    trade_date_list = jqdatasdk.get_trade_days(start_date=start_date, end_date=end_date)

    i = 0
    for trade_date in trade_date_list:
        if i == 0:
            i = 1
            continue

        symbols_df = jqdatasdk.get_all_securities(types=['futures'], date=trade_date)

        for index, row in symbols_df.iterrows():
            # 下载合约的日线数据
            downDailyBarBySymbol(index, row, str(trade_date_list[i - 1]))
            # 下载合约的分钟线数据
            downMinuteBarBySymbol(index, row, str(trade_date_list[i]), str(trade_date_list[i-1]))

        i += 1

    print('-' * 50)
    print(u'合约分钟线数据下载完成')
    print('-' * 50)
    return

#----------------------------------------------------------------------
# 合约列表补全数据
def downloadBarByDate(symbols_list, start_date, end_date=datetime.today().date()):
    jqdatasdk.auth(JQDATA_USER, JQDATA_PASSWORD)
    """下载所有配置中的合约的分钟线数据"""
    print('-' * 50)
    print(u'开始下载合约分钟线数据')
    print('-' * 50)

    symbols_map = {}
    for symbol in symbols_list:
        symbols_map[symbol] = 1

    trade_date_list = jqdatasdk.get_trade_days(start_date=start_date, end_date=end_date)

    i = 0
    for trade_date in trade_date_list:
        if i == 0:
            i = 1
            continue

        symbols_df = jqdatasdk.get_all_securities(types=['futures'], date=trade_date)

        for index, row in symbols_df.iterrows():
            if row['name'] in symbols_map.keys():
                # 下载合约的日线数据
                downDailyBarBySymbol(index, row, str(trade_date_list[i - 1]))
                # 下载合约的分钟线数据
                downMinuteBarBySymbol(index, row, str(trade_date_list[i]), str(trade_date_list[i-1]))

        i += 1

    print('-' * 50)
    print(u'合约分钟线数据下载完成')
    print('-' * 50)
    return
    