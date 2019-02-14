# -*- coding: utf-8 -*-
# @Time    : 2019-02-14 14:09
# @Author  : Dingzh.tobest
# 文件描述  ：按合约校验本地数据，并补全缺失的数据


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

def checkHistoryData(symbols_list, start_date, end_date=datetime.today().date()):
    jqdatasdk.auth(JQDATA_USER, JQDATA_PASSWORD)
    # 获取需要校验的合约的信息
    symbols_df = jqdatasdk.get_all_securities(types=['futures'])
    symbols_df = symbols_df[symbols_df['name'].isin(symbols_list)]

    mc = MongoClient(MONGO_HOST, MONGO_PORT)  # Mongo连接
    minute_db = mc[MINUTE_DB_NAME]  # 分钟数据库
    daily_db = mc[DAILY_DB_NAME]  # 日线数据库

    err_str = ''

    # 按日校验合约的日线和分钟线数据
    for symbol_index, symbol_row in symbols_df.iterrows():
        vt_symbol = symbol_row['name']
        print('start==>' + vt_symbol)
        prices_df = jqdatasdk.get_price(symbol_index, start_date=start_date, end_date=end_date, frequency='daily',
                                        fields=['open', 'close', 'high', 'low'])

        prices_df = prices_df.dropna()

        daily_error_list = []
        minute_error_list = []
        last_count = 0
        prices_df['next_trade_day'] = prices_df.index
        prices_df['next_trade_day'] = prices_df['next_trade_day'].shift(-1)

        symbol_daily_db = daily_db[vt_symbol]
        symbol_minute_db = minute_db[vt_symbol]

        for index, row in prices_df.iterrows():
            date = str(index)[:10].replace('-', '')
            print('开始校验数据：' + date)
            # 日线数据校验
            #     print("校验日线数据")
            daybar_count = symbol_daily_db.find({"date": date}).count()
            if daybar_count != 1:
                # print('日线数据错误：' + date + '当日数据量不符==>' + str(daybar_count))
                daily_error_list.append(date)

                # 分钟线数据校验
                #     print("校验分钟线数据")
            day_count = symbol_minute_db.find({"date": date}).count()
            if day_count == 0:
                # print('分钟线数据错误：' + date + '当日数据量为0')
                minute_error_list.append(date)
                continue
            elif day_count != last_count:
                df = jqdatasdk.get_price(symbol_index, start_date=str(index)[:10], end_date=row['next_trade_day'],
                                         frequency='minute', fields=['close'])
                if len(df) != day_count:
                    # print('分钟线数据错误：' + date + '当日数据量不符==>' + str(day_count) + ', 实际数量==>' + str(len(df)))
                    minute_error_list.append(date)
                    continue

            last_count = day_count

        if len(daily_error_list) != 0 or len(minute_error_list) != 0:
            err_fw = open(vt_symbol + '.error', 'w')
            err_fw.write('日线错误\r\n')
            err_fw.write('\r\n'.join(daily_error_list))
            err_fw.write('\r\n分钟线错误\r\n')
            err_fw.write('\r\n'.join(minute_error_list))
            err_fw.flush()
            err_fw.close()

            err_str = err_str + vt_symbol + ', error_info : ' + str(len(daily_error_list)) + ', minute_err_info : ' + str(len(minute_error_list)) + '\r\n'

    print(err_str)


if __name__ == '__main__':
    checkHistoryData(['I8888', 'RB8888'], '2018-01-01', '2019-02-13')