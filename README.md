# 聚宽历史行情服务

提供downloadAllMinuteBar()，可以通过定时任务的形式，按vnpy的数据格式，每日获取分钟数据写入到mongodb当中

提供downloadMinuteBarByDate，可以输入开始日期与结束日期，将时间段内的分钟数据写入到mongodb当中

loadHistroyData，可以将下载下来的所有历史数据的csv，增加到mongodb数据库当中

loadHistroyData进行修改，添加了线程池和修改了mongodb的导入部分，提高导入效率

感谢vnpy与jqdatasdk提供的开源的工具与数据，从中获得的技术和思路收益匪浅

## 2019-04-08
1. 增加了data_server_test.py，用于数据服务的单元测试，mock了jqdata的数据接口

## 2019-02-14内容更新
1. 增加checkHistoryData的数据校验功能，可以校验你希望校验的合约的日线和分钟线信息，保证本地数据的准确性，综合信息输出在控制台，具体合约错误的日期输出到本地以合约名命名的文件当中。
2. 增加按合约补全数据的功能，这样校验出来的错误就可以通过此功能补全了。

## 2018-12-10内容更新
1. 增加日线按日期进行补全的部分，现在调用downloadBarByDate，可以按日期来补全日线和分钟线了

## 2018-12-07内容更新
1. 增加日线历史数据导入的部分
2. 增加example中日线数据文件与分钟线数据文件的示例，用于表述导入时所使用的文件格式

# Dingzh.Tobest