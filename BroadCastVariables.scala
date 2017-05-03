package exercise

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Example of using Broadcast Variables.
  * spark-submit --class exercise.RevByProductWithBroadcast --num-executors 2 --master yarn helloworld_2.10-1.0.jar prod /user/sarangdp/data/retail_db /user/sarangdp/data/output/8 --conf spark-ui-port=22322
  */
object RevByProductWithBroadcast {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Rev Per Products using Broadcast")
      .setMaster(props.getConfig(args(0)).getString("executionMode"))
    val sc = new SparkContext(conf)

    val inputPath = args(1)
    val outputPath = args(2)

    val fs :FileSystem = FileSystem.get(sc.hadoopConfiguration)
    val op = new Path(outputPath)
    if(!fs.exists(new Path(inputPath))){
      println("Invalid input path")
    }

    if(fs.exists(op)){
      fs.delete(op, true)
    }

    val products = sc.textFile(inputPath+"/products").map(rec => (rec.split(",")(0).toInt, rec.split(",")(2)))

    val productsMap = products.collectAsMap()

    sc.broadcast(productsMap)

    val orderItems = sc.textFile(inputPath+"/order_items").map(rec => {
      val r = rec.split(",")
      (productsMap.get(r(2).toInt).get, r(4).toFloat)
    })

    orderItems.reduceByKey((acc,value) => acc+value).saveAsTextFile(outputPath)
  }

}
