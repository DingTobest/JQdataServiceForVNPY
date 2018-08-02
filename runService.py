# encoding: UTF-8

"""
定时服务，可无人值守运行，实现每日自动下载更新历史行情数据到数据库中。

"""
from __future__ import print_function

import time
import datetime

from dataService import downloadAllMinuteBar


if __name__ == '__main__':
    taskCompletedDate = None
    
    # 由于jqdata每晚20:20提供输出，定时任务从20:30开始执行
    taskTime = datetime.time(hour=20, minute=30)

    # 进入主循环
    while True:
        t = datetime.datetime.now()
        
        # 每天到达任务下载时间后，执行数据下载的操作
        if t.time() > taskTime and (taskCompletedDate is None or t.date() != taskCompletedDate):
            # 下载1000根分钟线数据，足以覆盖过去两天的行情
            downloadAllMinuteBar()
            
            # 更新任务完成的日期
            taskCompletedDate = t.date()
        else:
            print(u'当前时间%s，任务定时%s' %(t, taskTime))
    
        time.sleep(60)