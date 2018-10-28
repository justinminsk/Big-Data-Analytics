import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler


object myFunc {
	def cluster(data: DataFrame, k : Int): DataFrame={
		
			val assembler = new VectorAssembler()
                  .setInputCols(Array("doubles","homeruns"))
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


object final2 {
 def main(args: Array[String]) {
 	
 	val spark = SparkSession.builder().getOrCreate()
	
	val points = (
	spark.read.format("csv")
	.option("header", "false")
	.option("inferSchema", "true")
	.load("gs://justinminsk_bucket/final/final.csv")
	.select("_c16", "_c23", "_c25")
	.groupBy(col("_c16").alias("park"))
	.agg(sum("_c23").alias("doubles"), sum("_c25").alias("homeruns"))
	)
	
	/**myFunc.cluster(points, 4)
			.select("park", "doubles", "homeruns", "featureVector")
			.coalesce(1)
			.write.format("csv")
			.option("header", "true")
			.save("gs://justinminsk_bucket/final/output")*/
			
			val assembler = new VectorAssembler()
                  .setInputCols(Array("doubles","homeruns"))
                  .setOutputCol("featureVector")
            
            val scaler = new StandardScaler()
  						.setInputCol("featuresVector")
  						.setOutputCol("scaledFeatures")
  						.setWithStd(true)
  						.setWithMean(false)
            
            val kmeans = new KMeans()
                       .setK(4)
                       .setPredictionCol("cluster")
                       .setFeaturesCol("scaledFeatures")
                       
            val pipeline = new Pipeline().setStages(Array(assembler, scaler, kmeans))
			val pipelineModel = pipeline.fit(points)
			
			
			pipelineModel.transform(points).where(col("cluster")===3).show()
			
    
}
 }
