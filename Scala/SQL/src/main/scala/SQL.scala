import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
 

object SQL {
 def main(args: Array[String]) {
 	val spark = SparkSession.builder().getOrCreate()
    val sc=spark.sparkContext

				spark.read.format("csv")
            	.option("header","true")
	            .option("inferSchema", "true")
	            .load("gs://justinminsk_bucket/midterm/kddR")
	            .select("_c41", "_c4", "_c5")
	            .groupBy("_c41")
	            .agg(round(mean("_c4"), 1).alias("avgSent"), round(mean("_c5"), 1).alias("avgRec"), count("_c41").alias("total"))
	            .coalesce(1)
				.write.format("csv")
				.option("header","true")
				.save("gs://justinminsk_bucket/midterm/sqlAnswer.csv")
				
}
} 