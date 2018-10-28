#!/usr/bin/python

import pyspark

header = ['nameFirst','nameLast','teamName','playerID','yearID','stint','teamID','lgID','G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','SO','IBB','HBP','SH','SF','GIDP']


def toString(t):
	t[0].split[',']
	t.append(str(t[1]))
	return(','.join(t))


sc=pyspark.SparkContext()

bio=(
sc.textFile('gs://justinminsk_bucket/baseball/bio.csv')
.map(lambda s:s.split(','))
.collect()
)

bio=dict(bio)

(
sc.textFile('gs://justinminsk_bucket/baseball/battingext.csv')
.map(lambda s:s.split(','))
.map(lambda a:dict(zip(header,a)))
.map(lambda d:('{},{},{}'.format(d['nameFirst'],d['nameLast'],d['playerID']),int(d['HR'])))
.groupByKey()
.mapValues(sum)
.filter(lambda t:t[1] >= 500)
.sortBy(lambda t:t[1],ascending=False)
.map(lambda t:t[0] + ',' + str(t[1]))
.saveAsTextFile('gs://justinminsk_bucket/classwork/output')
)
