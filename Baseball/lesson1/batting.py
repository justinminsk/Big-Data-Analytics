#!/usr/bin/python

import pyspark

header = ['nameFirst','nameLast','teamName','playerID','yearID','stint','teamID','lgID','G','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','SO','IBB','HBP','SH','SF','GIDP']

sc=pyspark.SparkContext()

(
sc.textFile('gs://justinminsk_bucket/baseball/battingext.csv')
.map(lambda s:s.split(','))
.map(lambda a:dict(zip(header,a)))
.map(lambda d:(d['nameFirst'],d['nameLast'],d['playerID'],d['yearID'],d['HR']))
.filter(lambda t:int(t[4]) >= 45)
.sortBy(lambda t:t[4],ascending=False)
.map(lambda t:','.join(t))
.saveAsTextFile('gs://justinminsk_bucket/classwork/output')
)
