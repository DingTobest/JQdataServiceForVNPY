# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from dataService import *



if __name__ == '__main__':
    # 每日任务下载
    downloadAllMinuteBar()

    # 按日期补齐数据
    # downloadMinuteBarByDate('2010-01-01', '2011-01-01')