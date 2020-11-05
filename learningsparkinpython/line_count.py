#coding:utf-8

from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("my app")
sc = SparkContext(conf=conf)
lines = sc.textFile('D:/code/spark/input.txt')
pythonLines = lines.filter(lambda line: "python" in line)

print(lines.count())
print(pythonLines.first())