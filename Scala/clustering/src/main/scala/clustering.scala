import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
 

object myFunc {
	def cluster(data: DataFrame, k : Int): DataFrame\**Double*\={
		
			val assembler = new VectorAssembler()
                  .setInputCols(Array("x","y"))
                  .setOutputCol("featureVector")
                  
            val kmeans = new KMeans()
                       .setK(k)
                       .setPredictionCol("cluster")
                       .setFeaturesCol("featureVector")
                       
            val pipeline = new Pipeline().setStages(Array(assembler,kmeans))
			val pipelineModel = pipeline.fit(data)
			
			pipelineModel.transform(data)
			
			//val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
			//kmeansModel.computeCost(assembler.transform(data)) / data.count()
	}
}


object clustering {
 def main(args: Array[String]) {
	val spark = SparkSession.builder().getOrCreate()
	
	val points = (
	spark.read.format("csv")
	.option("header", "true")
	.option("inferSchema", "true")
	.load("gs://justinminsk_bucket/clustering/points.csv")
	)

	myFunc.cluster(points, 5)
				.select("x", "y", "label", "cluster")
				.coalesce(1)
				.write.format("csv")
				.option("header", "true")
				.save("gs://justinminsk_bucket/clustering/points_output2.csv")
	                
 	/**pipelineModel.transform(points)
				.select("x", "y", "label", "cluster")
				.coalesce(1)
				.write.format("csv")
				.option("header", "true")
				.save("gs://justinminsk_bucket/clustering/points_output.csv")
    
    for(k <- (2 to 10 by 1)) {
    	
    	println(myFunc.cluster(points,k))
    }*/
	   
}
 }
