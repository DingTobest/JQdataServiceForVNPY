# -*- coding: utf-8 -*-
# @Time    : 2019-04-08 10:06
# @Author  : Dingzh.tobest
# 文件描述  ：jqdata的数据服务测试

from dataService import *

import mock
import unittest
import jqdatasdk
import pandas as pd


class TestJQDataService(unittest.TestCase):

    # 初始化准备过程
    def setUp(self):
        print('----------JQDataService单元测试开始----------')
        self.data_init()
        self.mock_init()
        print('--------------------------------------------')

    # 测试结束，清除测试数据
    def tearDown(self):
        print('----------JQDataService单元测试结束----------')
        self.clear_data()

    def data_init(self):
        print('测试数据初始化==>')
        self.test_date_str = '2018-12-07'

        info_dict = {
            'display_name': '测试合约',
            'name': 'test88',
            'start_date': '2001-01-01',
            'end_date': '2030-01-01',
            'type': 'futures'
        }

        self.info_df = pd.DataFrame(info_dict, index=['TEST8888.CCFX'])
        self.info_df['start_date'] = pd.to_datetime(self.info_df['start_date'])
        self.info_df['end_date'] = pd.to_datetime(self.info_df['end_date'])

    def mock_init(self):
        print('mock初始化==>')
        # 日线数据的get_pirce初始化
        open_price = 20800.0
        close_price = 21440.0
        high_price = 21465.0
        low_price = 20735.0
        volume = 499394.0
        money = 52675250000

        self.daily_price = {'open': open_price,
                       'close': close_price,
                       'high': high_price,
                       'low': low_price,
                       'volume': volume,
                       'money': money}

        index = [self.test_date_str]
        date_index = pd.to_datetime(index)
        dp_df = pd.DataFrame(self.daily_price, index=date_index)

        jqdatasdk.get_price = mock.Mock(return_value=dp_df)

    def clear_data(self):
        print('清理测试数据==>')
        daily_db['test88'].drop()

    def test_downDailyBarBySymbol(self):
        print('测试案例==>downDailyBarBySymbol 执行')
        downDailyBarBySymbol('TEST8888.CCFX', self.info_df.iloc[0], self.test_date_str)
        test_datetime = datetime.strptime(self.test_date_str, '%Y-%m-%d')
        db_data = daily_db['test88'].find({'datetime':test_datetime})
        self.assertEqual(db_data[0]['open'], self.daily_price['open'])
        self.assertEqual(db_data[0]['close'], self.daily_price['close'])
        self.assertEqual(db_data[0]['high'], self.daily_price['high'])
        self.assertEqual(db_data[0]['low'], self.daily_price['low'])
        self.assertEqual(db_data[0]['volume'], self.daily_price['volume'])

if __name__ == '__main__':
    unittest.main()