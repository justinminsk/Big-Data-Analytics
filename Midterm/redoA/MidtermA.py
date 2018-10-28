#!/usr/bin/python
 
import pyspark

sc=pyspark.SparkContext()

def to_string(tuple):
	name = tuple[0]
	sent = tuple[1][0]
	rec = tuple[1][1]
	count = tuple[1][2]
	return "{},{},{},{}".format(name,sent,rec,count)

aTuple = (0,0,0)

pipeLine=(
sc.textFile('gs://justinminsk_bucket/midterm/kddR')
.map(lambda line: line.split(","))
.map(lambda array: (array[41],(long(array[4]),long(array[5]))))
.aggregateByKey(aTuple, lambda a,b: (a[0] + b[0],    a[1] + b[1], a[2] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
.mapValues(lambda v: (v[0]/v[2], v[1]/v[2], v[2]))
.map(to_string)
.saveAsTextFile('gs://justinminsk_bucket/midterm/betterMidA.csv')
)


print pipeLine