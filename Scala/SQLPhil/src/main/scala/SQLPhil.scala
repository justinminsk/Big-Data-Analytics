import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
 

object myFunction{
		def processToTup(s:String): (String,Int,Int,Int)={
			val id = s.split(",")(3)
			val hit = s.split(",")(6)(0)
			if(hit=='D'){
				(id,1,0,0)
			}else if(hit=='T'){
				(id,0,1,0)
			}else{
				(id,0,0,1)
			}
		}
}
 

object SQLPhil {
 def main(args: Array[String]) {
 	val spark = SparkSession.builder().getOrCreate()
 	val sc=spark.sparkContext
 	import spark.implicits._
 	
 	 val homeGames = (
 	 sc.textFile("gs://justinminsk_bucket/streaming/philGames")
 	 .filter(s=>s.slice(3,6) == "PHI")
 	 .map(s=>s.split("\\*EOL\\*"))
 	 .flatMap(s=>s)
 	 .filter(s=>s.slice(0,4) == "play")
 	 .filter(s=>s.split(",")(2) == "1")
 	 .filter(s=>s.split(",")(6)(0) == 'H' || s.split(",")(6)(0) == 'D' || s.split(",")(6)(0) == 'T')
 	 .filter(s=>s.split(",")(6)(1) != 'P')
 	 .filter(s=>s.split(",")(6)(1) != 'I')
 	 .map(myFunction.processToTup)
 	 .toDF("playerID", "D", "T", "HR")
 	 )
 	 
 	 val awayGames = (
 	 sc.textFile("gs://justinminsk_bucket/streaming/philGames")
 	 .filter(s=>s.slice(3,6) != "PHI")
 	 .map(s=>s.split("\\*EOL\\*"))
 	 .flatMap(s=>s)
 	 .filter(s=>s.slice(0,4) == "play")
 	 .filter(s=>s.split(",")(2) == "0")
 	 .filter(s=>s.split(",")(6)(0) == 'H' || s.split(",")(6)(0) == 'D' || s.split(",")(6)(0) == 'T')
 	 .filter(s=>s.split(",")(6)(1) != 'P')
 	 .filter(s=>s.split(",")(6)(1) != 'I')
 	 .map(myFunction.processToTup)
 	 .toDF("playerID", "D", "T", "HR")
 	 )

 	 val games =(
 	 homeGames.union(awayGames)
 	 .groupBy("playerID")
 	 .agg(sum("D").alias("totalD"), sum("T").alias("totalT"), sum("HR").alias("totalHR"))
 	 .show(100)
 	 )
 	 
 
}
 }
