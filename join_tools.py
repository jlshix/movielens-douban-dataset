# coding: utf-8
# Created by leo on 17-5-13.
# 使用豆瓣爬虫数据集和 movielens 数据集共有的 imdb 字段求交集

from pymongo import MongoClient
from pyspark import SparkConf, SparkContext


def get_imdb_id(sc, size):
    """
    从 links.csv 获取数据
    :param sc: spark context
    :param size: small or full
    :return: [(imdbId, movieId), ...]
    """
    dic = {
        'small': 'ml-latest-small',
        'full': 'ml-latest'
    }
    # 读取 csv -> 分行切割 -> 每行变为一个列表 -> (imdb, id) 元组
    return sc.textFile('./{}/links.csv'.format(dic[size])) \
        .flatMap(lambda x: x.split('\n')) \
        .map(lambda x: x.split(',')) \
        .map(lambda x: (x[1], x[0]))


def get_imdb_douban(col, sc):
    """
    从 MongoDB 获取数据
    :param col: 集合
    :param sc: spark context
    :return: [(imdbId, doubanId), ...]
    """
    # 查询 imdb 字段不为空的电影, 结果只取所需字段
    query = col.find({'imdb': {'$ne': ''}}, {'_id': 1, 'imdb': 1})
    # 去掉 tt前缀并改变顺序 成为 (imdbId, doubanId) 元组
    tmp = map(lambda x: (x['imdb'][2:], x['_id']), query)
    return sc.parallelize(tmp)


def to_mongo(old, new, res):
    """
    选取重合数据存入新的数据库集合中
    :param old: old collection
    :param new: new collection
    :param res: join res from spark
    """
    # (imdb, (id, douban)) -> (id, douban)
    ids = res.map(lambda x: x[1]).toLocalIterator()
    for lens_id, douban in ids:
        doc = old.find_one({'_id': douban})
        doc['lens_id'] = int(lens_id)
        new.insert_one(doc)


def to_movie(sc, size, movie_ids):
    """得到筛选后的 电影数据集
    """
    dic = {
        'small': 'ml-latest-small',
        'full': 'ml-latest'
    }
    sc.textFile('./{}/movies.csv'.format(dic[size])) \
        .flatMap(lambda x: x.split('\n')) \
        .map(lambda x: x.split(',')) \
        .filter(lambda x: x[0] in movie_ids)\
        .map(lambda x: ','.join(x))\
        .saveAsTextFile('movies')


def to_rating(sc, size, movie_ids):
    """得到筛选后的 评分数据集
    """
    dic = {
        'small': 'ml-latest-small',
        'full': 'ml-latest'
    }
    sc.textFile('./{}/ratings.csv'.format(dic[size])) \
        .flatMap(lambda x: x.split('\n')) \
        .map(lambda x: x.split(',')) \
        .filter(lambda x: x[1] in movie_ids) \
        .map(lambda x: u'{},{},{},{}'.format(x[0], x[1], x[2], x[3])) \
        .saveAsTextFile('ratings')


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    col = client['movie']['spider']
    new = client['mr']['movie']
    conf = SparkConf().setAppName('Join IMDB').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    imdb_id = get_imdb_id(sc, 'full')
    print imdb_id.count(), imdb_id.take(5)
    imdb_douban = get_imdb_douban(col, sc)
    print imdb_douban.count(), imdb_douban.take(5)
    # (imdb, id).(imdb, douban) -> (imdb, (id, douban))
    res = imdb_id.join(imdb_douban)
    print res.count()

    to_mongo(col, new, res)

    movie_ids = res.map(lambda x: x[1][0])
    to_movie(sc, 'full', list(movie_ids.toLocalIterator()))
    to_rating(sc, 'full', list(movie_ids.toLocalIterator()))
