import org.apache.spark.SparkContext
 
object myFunction {
	
	def toKeyValue(a:Array[String]): (String, Int)={
		val date = a(0).slice(3,11)
		(date + " " + a(4),1)
	}
	
	def toIDName(a:Array[String]): (String,String) = {
		if(a.lift(2) != None) {
			(a(0),a(2) + " " + a(1))
		}
		else {
			(a(0),a(1))
		}
	}
	
	def toFinal(t:(String,Int)): String = {
		val year = t._1.slice(0,4)
		val month = t._1.slice(0,4) + "-" + t._1.slice(4,6)
		val day = t._1.slice(0,4) + "-" + t._1.slice(4,6) + "-" + t._1.slice(6,8)
		val name = t._1.split(" ").drop(1).mkString(" ")
		val hr = t._2
		year + "," + month + "," + day + "," + name + "," + hr
	}
	
}

object Practice {
 def main(args: Array[String]) {
	val sc=new SparkContext()
	
	val players = (
		sc.textFile("gs://justinminsk_bucket/classwork/retroID")
		.map(string => string.split(","))
		.map(myFunction.toIDName)
		.collect()
	)
	
	val pMap = players.toMap
	
	val result =(
 		sc.textFile("gs://justinminsk_bucket/classwork/scala/part*")
		 .map(string => string.split(","))
		 .filter(array => array(1)=="play")
		 .filter(array => array(7)(0)=='H' && array(7)(1)!='P')
		 .map(myFunction.toKeyValue)
		 .groupByKey()
		 .mapValues(i => i.sum)
		 .map(tuple => (tuple._1 + " " + pMap(tuple._1.split(" ")(1)),tuple._2))
		 .map(myFunction.toFinal)
		 .saveAsTextFile("gs://justinminsk_bucket/classwork/scalaResults/")
 		)
 		
 
}
 }
