package exercise

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

case class YahooStockPrice(date:String, open:Float, high:Float, low:Float, close:Float, volumne:Int, adjClose:Float)

object YahooStocks {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("ORC format Test")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val inputPath = args(0)
    val outputPath = args(1)

    val fs :FileSystem = FileSystem.get(sc.hadoopConfiguration)
    val op = new Path(outputPath)
    if(!fs.exists(new Path(inputPath))){
      println("Invalid input path")
    }

    if(fs.exists(op)){
      fs.delete(op, true)
    }
    val yahoo_stocks = sc.textFile(inputPath+"/yahoo_stocks.csv")
    val header = yahoo_stocks.first
    val data = yahoo_stocks.mapPartitionsWithIndex((idx, itr) => if(idx==0) itr.drop(1) else itr)

    val stockPrice = data.map(_.split(",")).map(row => YahooStockPrice(row(0), row(1).toFloat, row(2).toFloat,row(3).toFloat,row(4).toFloat,row(5).toInt,row(6).toFloat)).toDF
    stockPrice.registerTempTable("yahoo_stocks_temp")
    val results = hiveContext.sql("select * from yahoo_stocks_temp")
    results.write.mode("overwrite").format("orc").save(outputPath+"/yahoo_stocks_orc")

    val yahoo_stock_orc = hiveContext.read.format("orc").load(outputPath+"/yahoo_stocks_orc")
    yahoo_stock_orc.registerTempTable("orcTest")

    hiveContext.sql("select * from orcTest").rdd.saveAsTextFile(outputPath+"/finalOutput")

  }
}
