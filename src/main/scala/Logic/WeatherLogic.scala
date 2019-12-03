package Logic

import Models.{DataBlock, WeatherData}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import java.lang.System.currentTimeMillis

object WeatherLogic {
  def calcAvgTemp(rdd: RDD[DataBlock]): Option[Double] = {
    if (!rdd.isEmpty()) {
      val filteredRdd = rdd
        .flatMap(x => x.data)
        .filter(x => x.temperature.isDefined)
      if (!filteredRdd.isEmpty()) {
        Some(filteredRdd.map(x => x.temperature.get).sum() / filteredRdd.count())
      }
      else
        None
    }
    else
      None
  }

  def fahrenheitToCelsius(fahrTemp: Double): Double = { fahrTemp - 32.0 }

  def calcAvgMinMaxTemp(rdd: RDD[DataBlock]): Option[(Double, Double)] = {
    if (!rdd.isEmpty()) {
      val filteredRdd = rdd
        .flatMap(x => x.data)
        .filter(x => x.temperatureMax.isDefined && x.temperatureMin.isDefined)
      if (!filteredRdd.isEmpty()) {
        val maxTempSum = filteredRdd.map(x => x.temperatureMax.get).sum()
        val minTempSum = filteredRdd.map(x => x.temperatureMin.get).sum()
        val size = filteredRdd.count()
        Some((minTempSum / size, maxTempSum / size))
      }
      else
        None
    }
    else None
  }

  def getHourlyInfo(hourlyForecast: DataBlock, city: String, output: Boolean = false): List[Option[Double]] = {
    val definedTemp: List[Double] = hourlyForecast.data.filter(x => x.temperature.isDefined).map(x => x.temperature.get)
    val avgTemp = if (definedTemp.nonEmpty) {
      val avg = definedTemp.sum / definedTemp.size
      if (output) println(s"[$city] Average hourly temperature: $avg")
      Some(fahrenheitToCelsius(avg))
    }
    else
      None

    val definedApTemp = hourlyForecast.data.filter(x => x.apparentTemperature.isDefined)
      .map(x => x.apparentTemperature.get)
    val avgApTemp = if (definedApTemp.nonEmpty) {
      val avg = definedApTemp.sum / definedApTemp.size
      if (output) println(s"[$city] Average hourly apparent temperature: $avg")
      Some(fahrenheitToCelsius(avg))
    }
    else
      None

    List(avgTemp, avgApTemp)
  }

  def getDailyInfo(dailyForecast: DataBlock, city: String, output: Boolean = false): List[Option[Double]] = {
    val definedMinTemp = dailyForecast.data.filter(x => x.temperatureMin.isDefined).map(x => x.temperatureMin.get)

    val avgMinTemp = if (definedMinTemp.nonEmpty) {
      val avg = definedMinTemp.sum / definedMinTemp.size
      if (output) println(s"[$city] Average daily min temperature: $avg")
      Some(fahrenheitToCelsius(avg))
    }
    else
      None

    val definedMaxTemp = dailyForecast.data.filter(x => x.temperatureMax.isDefined).map(x => x.temperatureMax.get)
    val avgMaxTemp = if (definedMaxTemp.nonEmpty) {
      val avg = definedMaxTemp.sum / definedMaxTemp.size
      if (output) println(s"[$city] Average daily max temperature: $avg")
      Some(fahrenheitToCelsius(avg))
    }
    else
      None

    val definedApTempHigh = dailyForecast.data.filter(x => x.apparentTemperatureHigh.isDefined)
      .map(x => x.apparentTemperatureHigh.get)
    val avgTempHigh = if (definedApTempHigh.nonEmpty) {
      val avg = definedApTempHigh.sum / definedApTempHigh.size
      if (output) println(s"[$city] Average daily high apparent temperature: $avg")
      Some(fahrenheitToCelsius(avg))
    }
    else
      None

    val definedApTempLow = dailyForecast.data.filter(x => x.apparentTemperatureLow.isDefined)
      .map(x => x.apparentTemperatureLow.get)
    val avgTempLow = if (definedApTempLow.nonEmpty) {
      val avg = definedApTempLow.sum / definedApTempLow.size
      if (output) println(s"[$city] Average daily low apparent temperature: $avg")
      Some(fahrenheitToCelsius(avg))
    }
    else
      None

    val definedCloudCover = dailyForecast.data.filter(x => x.cloudCover.isDefined)
      .map(x => x.cloudCover.get)
    val avgCloudCover = if (definedCloudCover.nonEmpty) {
      val avg = definedCloudCover.sum / definedCloudCover.size
      if (output) println(s"[$city] Average daily cloud cover: $avg")
      Some(avg)
    }
    else
      None


    List(avgMinTemp, avgMaxTemp, avgTempHigh, avgTempLow, avgCloudCover)
  }

  def extractCharacteristics(weatherData: DStream[WeatherData]): Unit = {
    weatherData.map{ x =>
      val city = x.city
      val hourlyInfo = getHourlyInfo(x.hourly, x.city, output = true).map(x => x.getOrElse("None")).toList
      val dailyInfo = getDailyInfo(x.daily, x.city, output = true).map(x => x.getOrElse("None")).toList
      (city :: hourlyInfo ::: dailyInfo).mkString(" ")
    }.repartition(1).saveAsTextFiles("results/forecast")
  }
}
