#!/usr/bin/python

import pyspark

sc=pyspark.SparkContext()

def to_string(tuple):
	partarray = tuple[0].split()
	count = tuple[1]
	return "{},{},{},{}".format(partarray[0],partarray[1],partarray[2],count)

aTuple = (0,0)

byteSent=(
sc.textFile('gs://justinminsk_bucket/midterm/kddR')
.map(lambda line: line.split(","))
.map(lambda array: (array[41],long(array[4])))
.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
.mapValues(lambda v: v[0]/v[1])
.collect()
)

dictByteSent = dict(byteSent)
aTuple = (0,0)

byteRec=(
sc.textFile('gs://justinminsk_bucket/midterm/kddR')
.map(lambda line: line.split(","))
.map(lambda array: (array[41],long(array[5])))
.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
.mapValues(lambda v: v[0]/v[1])
.collect()
)

dictByteRec = dict(byteRec)

count=(
sc.textFile('gs://justinminsk_bucket/midterm/kddR')
.map(lambda line: line.split(","))
.map(lambda array: (array[41] + ' ' + str(dictByteSent[array[41]]) + ' ' + str(dictByteRec[array[41]]),1))
.groupByKey()
.mapValues(sum)
.map(to_string)
.saveAsTextFile('gs://justinminsk_bucket/midterm/Aresult')
)

