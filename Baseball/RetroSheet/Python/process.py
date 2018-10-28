#!/usr/bin/python

import pyspark

def attach_id(array):
	id = array[0].split(',')[1]
	return map(lambda string: id + ',' + string, array)

sc=pyspark.SparkContext()

(
sc.textFile('gs://justinminsk_bucket/classwork/retroGames/*games')
.map(lambda line:line.split('*EOL*'))
.map(attach_id)
.flatMap(lambda string:string)
.saveAsTextFile('gs://justinminsk_bucket/classwork/V2RetroGames/output')
)

