import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object finalq1 {
 def main(args: Array[String]) {
 	
 	val spark = SparkSession.builder().getOrCreate()

	val yankeesBatting = (
				spark.read.format("csv")
            	.option("header","true")
	            .option("inferSchema", "true")
	            .load("gs://justinminsk_bucket/SQLScala/Batting.csv")
	            .where(col("teamID")==="NYA")
	            .where(col("yearID") >= 1920)
	            .select("teamID", "yearID", "HR", "SO", "RBI")
	            .groupBy(col("yearID"))
	            .agg(sum("HR").alias("Total_HR"), sum("SO").alias("Total_SO"), sum("RBI").alias("Total_RBI"))
	            )
	
	
	val sc=spark.sparkContext
	val rdd = yankeesBatting.rdd.map(row=>row.mkString(",")).collect()
	val rddQueue = new Queue[RDD[String]]
	

	for(game<-rdd){
        	rddQueue += sc.parallelize(Array(game))
        }

	val ssc = new StreamingContext(sc,Seconds(5))            
	val inputStream = ssc.queueStream(rddQueue)
	
	
	
	inputStream.window(Seconds(5),Seconds(5)).print()
	
	
	
	 
	ssc.start()
	Thread.sleep(50)
	ssc.stop()   
	
}
 }
