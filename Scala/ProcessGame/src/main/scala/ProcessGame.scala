import org.apache.spark.SparkContext

object Myfunction{
	def attach_id(a:Array[String]): Array[String]={
		val EntryOne = a(0).split(",")(1)
		a.map(s=>EntryOne+","+s)
	}

}

object Practice {
 def main(args: Array[String]) {
 val sc=new SparkContext()
(
 sc.textFile("gs://justinminsk_bucket/classwork/retroGames/*games")
 .map(s=>s.split("\\*EOL\\*"))
 .map(Myfunction.attach_id)
 .flatMap(s=>s)
 .saveAsTextFile("gs://justinminsk_bucket/classwork/Scala/processedGames/")
 )
}
 }
