# -*- coding: utf-8 -*-
# @Time    : 2018-08-06 14:35
# @Author  : Dingzh.tobest
# 文件描述  ： 加载历史数据到mongodb

# encoding: UTF-8

from __future__ import print_function
import sys
import json
from datetime import datetime
from time import time, sleep

from pymongo import MongoClient, ASCENDING

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME, DAILY_DB_NAME

import pandas as pd
import os
import threadpool, threading

# 加载配置
config = open('config.json')
setting = json.load(config)

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']

mc = MongoClient(MONGO_HOST, MONGO_PORT)  # Mongo连接
minute_db = mc[MINUTE_DB_NAME]  # 数据库
daily_db = mc[DAILY_DB_NAME]  # 数据库

futures_symbol_map = {}

# 分钟线数据路径
data_path = 'D:\\stockdata\\indexminuteprices'

daily_data_path = 'D:\\stockdata\\indexdailyprices2'

# data_path = 'D:\\stockdata\\futures\\minute'  # 测试路径

pos = 0     # 文件计数
count = 0   # 文件总数
last = 1  # 上次导入的位置,初始为1

pos_lock = threading.Lock()
file_lock = threading.Lock()


# ----------------------------------------------------------------------
# 生成分钟Bar
def generateVtBar(symbol, d):
    """生成K线"""
    bar = VtBarData()
    bar.vtSymbol = symbol
    bar.symbol = symbol
    bar.open = float(d['open'])
    bar.high = float(d['high'])
    bar.low = float(d['low'])
    bar.close = float(d['close'])
    bar.date = datetime.strptime(d['Unnamed: 0'][0:10], '%Y-%m-%d').strftime('%Y%m%d')
    bar.time = d['Unnamed: 0'][11:]
    bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
    bar.volume = d['volume']

    return bar
    # 另外一种文件格式
    # bar = VtBarData()
    # bar.vtSymbol = symbol
    # bar.symbol = symbol
    # bar.open = float(d['Open'])
    # bar.high = float(d['High'])
    # bar.low = float(d['Low'])
    # bar.close = float(d['Close'])
    # bar.date = datetime.strptime(d['Date'][0:10], '%Y-%m-%d').strftime('%Y%m%d')
    # bar.time = d['Time']
    # bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
    # bar.volume = d['TotalVolume']
    #
    # return bar

# 生成日Bar
def generateDailyVtBar(symbol, d):
    """生成K线"""
    bar = VtBarData()
    bar.vtSymbol = symbol
    bar.symbol = symbol
    bar.open = float(d['open'])
    bar.high = float(d['high'])
    bar.low = float(d['low'])
    bar.close = float(d['close'])
    bar.date = datetime.strptime(d['Unnamed: 0'], '%Y-%m-%d').strftime('%Y%m%d')
    bar.time = ''
    bar.datetime = datetime.strptime(bar.date, '%Y%m%d')
    bar.volume = d['volume']

    return bar

# 读取分钟线csv数据写入到
def loadCsvData(file_name):
    start = time()
    if file_lock.acquire():
        # symbol_name = file_name[0: -8]
        symbol_name = file_name[0: -4]
        file_path = data_path + '\\' + file_name
        print(u'合约%s数据开始导入' % (symbol_name))
        file_lock.release()

    if symbol_name[-4:] != '8888' and symbol_name[-4:] != '8888':
        if symbol_name[0: -4] in futures_symbol_map.keys():
            symbol_name = futures_symbol_map[symbol_name[0: -4]] + symbol_name[-4:]

    minute_df = pd.read_csv(file_path, encoding='GBK')
    global pos

    if pos_lock.acquire():
        pos += 1
        pos_index = pos
        pos_lock.release()
    if minute_df.empty:
        print(u'合约%s数据为空跳过，进度(%s / %s)' % (symbol_name, str(pos_index), str(count)))
        return

    cl = minute_db[symbol_name]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)  # 添加索引
    data_list = []
    for index, row in minute_df.iterrows():
        bar = generateVtBar(symbol_name, row)
        d = bar.__dict__
        data_list.append(d)

    cl.insert_many(data_list)

    e = time()
    cost = (e - start) * 1000

    print(u'合约%s数据导入完成，耗时%s毫秒，进度(%s / %s)' % (symbol_name, cost, str(pos_index), str(count)))

def loadDailyCsvData(file_name):
    start = time()
    if file_lock.acquire():
        symbol_name = file_name[0: -4]
        file_path = daily_data_path + '\\' + file_name
        print(u'合约%s数据开始导入' % (symbol_name))
        file_lock.release()

    if symbol_name[-4:] != '8888' and symbol_name[-4:] != '9999':
        if symbol_name[0: -4] in futures_symbol_map.keys():
            symbol_name = futures_symbol_map[symbol_name[0: -4]] + symbol_name[-4:]

    daily_df = pd.read_csv(file_path, encoding='GBK')
    global pos

    if pos_lock.acquire():
        pos += 1
        pos_index = pos
        pos_lock.release()
    if daily_df.empty:
        print(u'合约%s数据为空跳过，进度(%s / %s)' % (symbol_name, str(pos_index), str(count)))
        return

    cl = daily_db[symbol_name]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)  # 添加索引
    data_list = []
    for index, row in daily_df.iterrows():
        bar = generateDailyVtBar(symbol_name, row)
        d = bar.__dict__
        data_list.append(d)

    cl.insert_many(data_list)

    e = time()
    cost = (e - start) * 1000

    print(u'合约%s数据导入完成，耗时%s毫秒，进度(%s / %s)' % (symbol_name, cost, str(pos_index), str(count)))

# 加载分钟线的历史数据
def loadHistoryData():
    file_list = os.listdir(data_path)
    file_list = file_list[last - 1:]
    global pos
    pos = last
    # 上次添加到670， BU1512已导入
    global count
    count = len(file_list)
    # 增加4个线程的线程池，多线程来提高导入效率
    pool = threadpool.ThreadPool(1)
    requests = threadpool.makeRequests(loadCsvData, file_list)
    for req in requests:
        pool.putRequest(req)
    pool.wait()

    print('--------历史数据导入完成--------')

# 加载日线的历史数据
def loadDailyHistoryData():
    file_list = os.listdir(daily_data_path)
    file_list = file_list[last - 1:]
    global pos
    pos = last
    # 上次添加到670， BU1512已导入
    global count
    count = len(file_list)
    # 增加4个线程的线程池，多线程来提高导入效率
    pool = threadpool.ThreadPool(4)
    requests = threadpool.makeRequests(loadDailyCsvData, file_list)
    for req in requests:
        pool.putRequest(req)
    pool.wait()

    print('--------历史数据导入完成--------')

if __name__ == '__main__':
    # 加载字典信息，历史数据文件中品种都是大写，需要增加信息，将某些转化为小写
    print('------历史数据文件导入开始------')
    symbol_df = pd.read_csv('futures_type.csv', encoding='GBK')

    for index, row in symbol_df.iterrows():
        futures_symbol_map[row['type'].upper()] = row['type']
    print('字典信息加载完毕，开始导入历史数据')

    # 导入历史的日线数据
    loadDailyHistoryData()

    # 导入历史的分钟线数据
    loadHistoryData()


