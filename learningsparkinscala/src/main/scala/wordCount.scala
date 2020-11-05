import org.apache.spark.{SparkConf, SparkContext}

object wordCount {

    def main(args:Array[String]):Unit = {
      println("hello world")
      val conf=new SparkConf().setMaster("local").setAppName("wordCount")
      val sc=new SparkContext(conf)

      val input = sc.textFile("./input.txt")
      val words = input.flatMap(line=> line.split(" "))
      val counts=words.map(word=>(word,1)).reduceByKey{case (x,y) => x+y}

      counts.collect().foreach {println}
//      counts.saveAsTextFile(outputFile)

    }
}
