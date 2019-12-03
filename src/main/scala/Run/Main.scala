package Run

import Logic.StreamIO._
import Logic.WeatherLogic._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("ParComp-HW7")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    val dataStream = getStreamFromFs(ssc)
    val weatherStream = parseDataStreamFs(dataStream)

    extractCharacteristics(weatherStream)


    ssc.start()
    ssc.awaitTermination()
  }
}
