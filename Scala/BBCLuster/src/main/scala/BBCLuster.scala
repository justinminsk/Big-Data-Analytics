import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler


object myFunc{
	def cluster(data: DataFrame, k: Int): DataFrame ={
			val assembler = new VectorAssembler()
                  .setInputCols(Array("atBat", "hits", "doubles", "triples", "homeruns", "runsBattedIn", "walks"))
                  .setOutputCol("featureVector")
            
            val scaler = new StandardScaler()
                 .setInputCol("featureVector")
                 .setOutputCol("scaledFeatureVector")
                 .setWithStd(true)
                 .setWithMean(false)
            
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


object BBCLuster {
	def main(args: Array[String]) {
 		val spark = SparkSession.builder().getOrCreate()
   		val baseball=(
			spark.read.format("csv")
 			.option("header","true")
            .option("inferSchema", "true")
            .load("gs://justinminsk_bucket/SQLScala/Batting.csv")
            .groupBy("playerID")
            .agg(
           		sum("AB").alias("atBat"),
           		sum("H").alias("hits"),
           		sum("2B").alias("doubles"),
           		sum("3B").alias("triples"),
           		sum("HR").alias("homeruns"),
           		sum("RBI").alias("runsBattedIn"),
           		sum("BB").alias("walks")
           )
           .filter(col("atBat") >= 5000)
       )
       
		myFunc.cluster(baseball, 4)
		.filter(col("cluster") === 3)
		.select("playerID")
		.orderBy("playerID")
		.show(100)
       
}
 }
