import pyspark
import pyspark.streaming
from time import sleep
import requests


url = "http://things.ubidots.com/api/v1.6/devices/Plotter"
headers = {"X-Auth-Token": "A1E-9RlZ8bQCehCglBG3FuPVK4vlnGOXD3", "Content-Type": "application/json"}

def sendData(n):
    payload={"hr":n}
    requests.post(url=url, headers=headers, json=payload)

sc=pyspark.SparkContext()
ssc=pyspark.streaming.StreamingContext(sc,1)
rddQueue=[]

games=(
            sc.textFile("gs://justinminsk_bucket/streaming/philGames")
            .map(lambda s:s.split("*EOL*"))
            .sortBy(lambda a:a[5])
            .collect()
      )

for game in games:
            rddQueue += [sc.parallelize(['*EOL*'.join(game)])]
        
inputStream = ssc.queueStream(rddQueue)

result=(
            inputStream
            .window(10,1)
            .map(lambda s:s.split("*EOL*"))
            .flatMap(lambda s:s)
            .filter(lambda s:s[0:4]=="play")
            .filter(lambda s:s.split(",")[6][0]=='H')
            .filter(lambda s:s.split(",")[6][1]!='P')
            .count()           
)
result.foreachRDD(lambda rdd:rdd.foreach(sendData))
result.pprint()
ssc.start()
sleep(300)
ssc.stop()