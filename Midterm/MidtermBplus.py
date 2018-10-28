#!/usr/bin/python

import pyspark

sc=pyspark.SparkContext()

result=(
sc.textFile('gs://justinminsk_bucket/midterm/kddR')
.map(lambda line: line.split(","))
.map(lambda array: (array[41],1))
.groupByKey()
.mapValues(sum)
.saveAsTextFile('gs://justinminsk_bucket/midterm/Bresult')
)

print result