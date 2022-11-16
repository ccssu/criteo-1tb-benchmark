# coding:utf8
from pyspark import SparkConf,SparkContext

if __name__ =='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellWorld")
    # 通过SparkConf对象创建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求： wordcount 单词计数，读取 HDFS上的words.txt文件，统计单词出现的数量
    # 读取文件
    file_rdd = sc.textFile("./data.txt")

    # 将单词进行切割，得到一个存储全部单词的集合对象
    words_rdd = file_rdd.flatMap(lambda line:line.split(" "))
    # print(len(words_rdd.collect()))


    # 将单词进行元组对象，key为单词，value是数字1
    words_with_one_rdd = words_rdd.map(lambda x:(x,1))

    # 将元组的value，按照key来分组，对所有的value执行聚集操作(相加)
    result_rdd = words_with_one_rdd.reduceByKey(lambda a,b :a+b)

    # 通过collect 方法手动RDD的数据打印输出结果
    # collect方法，是将RDD(分布式对象)中的每个分区的数据，都发送到Driver中，形成一个Python List对象
    # collect: 分布式 转 -> 本地集合
    print(result_rdd.collect())