#!/usr/bin/python
 
import pyspark

sc=pyspark.SparkContext()

def to_string(tuple):
	year = tuple[0][3:7]
	month = tuple[0][3:7]+'-'+tuple[0][7:9]
	day = tuple[0][3:7]+'-'+tuple[0][7:9]+'-'+tuple[0][9:11]
	name = tuple[0][13:]
	hr = tuple[1]
	endArray = [year,month,day,name,hr]
	return '{},{},{},{},{}'.format(endArray[0],endArray[1],endArray[2],endArray[3],endArray[4])

players=(
sc.textFile('gs://justinminsk_bucket/classwork/retroID')
.map(lambda string : string.split(','))
.map(lambda array : (array[0],array[2] + ' ' + array[1]))
.collect()
)

players=dict(players)

(
sc.textFile('gs://justinminsk_bucket/classwork/V2RetroGames/output/part*')
.map(lambda line : line.split(','))
.filter(lambda array :array[1] == 'play')
.filter(lambda array : array[7][0] == 'H' and array[7][1] != 'P')
.map(lambda array : (array[0] + ' ' + array[4] + ' ' + players[array[4]], 1))
.groupByKey()
.mapValues(sum)
.map(to_string)
.saveAsTextFile('gs://justinminsk_bucket/classwork/output/')
)

