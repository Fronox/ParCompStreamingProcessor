package Logic

import Models.{Alert, DataBlock, DataPoint, WeatherData}
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, parser}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

object StreamIO {
  def getStreamFromFs(ssc: StreamingContext): DStream[String] = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDirectory = currentDirectory + "/data/"
    val data = ssc.textFileStream(dataDirectory)
    data
  }

  def parseDataStreamFs(stream: DStream[String]): DStream[WeatherData] = {
    val weatherData: DStream[WeatherData] = stream.map(x => {
      implicit val alertDecoder: Decoder[Alert] = deriveDecoder[Alert]
      implicit val dataPointDecoder: Decoder[DataPoint] = deriveDecoder[DataPoint]
      implicit val dataBlockDecoder: Decoder[DataBlock] = deriveDecoder[DataBlock]
      implicit val weatherDecoder: Decoder[WeatherData] = deriveDecoder[WeatherData]
      val result = parser.decode[WeatherData](x)
      result match {
        case Right(weatherData) => weatherData
        case Left(error) =>
          println(error)
          println(x)
          throw new Exception("Can not parse data")
      }
    })

    weatherData
  }
}
