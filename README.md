# 豆瓣数据集

## 概况

- 使用爬虫爬取豆瓣电影 48223 条数据 (spider.json)
- 与movielens ml-latest 数据含有共同的 imdb 字段
- 使用 spark rdd 的 join 操作共获取共同数据 15752 条 (movie.json)


## 数据说明

- 原数据存储于 MongoDB, 使用 mongoexport 命令导出为 movie.json
- 可使用 mongoimport 命令再次导入到 mongodb
- 亦可直接操作 json 或导入到其他数据库使用

- 对于 ml-latest 的 movies.csv 和 ratings.csv 也做了相应处理,仅保留交集电影数据
- 这部分分片保存在相应的文件夹中,未进行进一步处理


## 源数据

- movielens 数据来自[这个页面](https://grouplens.org/datasets/movielens/)
- 使用的数据集为 [ml-latest](http://files.grouplens.org/datasets/movielens/ml-latest.zip)
- 使用爬虫爬取的源数据保存为 spider.json

## join_tools.py 运行条件

- 需获取 movielens 数据解压至同目录
- 将 spider.json 数据导入至 MongoDB
- 按需修改并使用相应函数即可


有问题或改进建议请 issue
欢迎 star :)
