import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds	


object streaming {	
    def main(args: Array[String]) {
        val sc = new SparkContext()
        val ssc = new StreamingContext(sc,Seconds(1))
        val rddQueue = new Queue[RDD[String]]

        val philGames = (
        	sc.textFile("gs://justinminsk_bucket/streaming/philGames")
        	.map(s => s.split("\\*EOL\\*"))
        	.sortBy(a => a(5))
 			.collect()	
        )
         
        for(game<-philGames){
        	rddQueue += sc.parallelize(Array(game.mkString("*EOL*")))
        }
        
     	val inputStream = ssc.queueStream(rddQueue)
        
        (
        	inputStream
        	.map(s => s.split("\\*EOL\\*"))
 			.flatMap(s=>s)
		 	.filter(s => s.slice(0,4)=="play")
			.filter(s => s.split(",")(6)(0)=='H')
			.filter(s => s.split(",")(6)(1)!='P')
			.window(Seconds(10),Seconds(10))
			.count()
        	.print()
        )
        
        ssc.start()
		Thread.sleep(1630000)
		ssc.stop()   
    }
}

     