package com.pixipanda.producer

import java.io.{BufferedReader, FileReader}
import java.util.{Date, Properties, Random}

import Constants._
import com.google.gson.{Gson, JsonObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class AdvLogProducer (brokers:String, adImpressionTopic:String, adClickTopic:String) extends  Thread{

  val props = createProducerConfig(brokers)
  val producer = new KafkaProducer[String, String](props)


  def createProducerConfig(brokers: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    /*    props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);*/
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  
  def adImpressiontoJson(adImpression: AdImpression) = {

    val adImpJsonObj = new JsonObject
    val gson: Gson = new Gson
    adImpJsonObj.addProperty("id", adImpression.id)
    adImpJsonObj.addProperty("timestamp", adImpression.timestamp.toString)
    adImpJsonObj.addProperty("publisher", adImpression.publisher)
    adImpJsonObj.addProperty("advertiser", adImpression.advertiser)
    adImpJsonObj.addProperty("website", adImpression.website)
    adImpJsonObj.addProperty("geo", adImpression.geo)
    adImpJsonObj.addProperty("bid", adImpression.bid)
    adImpJsonObj.addProperty("cookie", adImpression.cookie)

    gson.toJson(adImpJsonObj)
  }


  def adClicktoJson(adClick: AdClick) = {

    val adClickJsonObj = new JsonObject
    val gson: Gson = new Gson
    adClickJsonObj.addProperty("id", adClick.id)
    adClickJsonObj.addProperty("timestamp", adClick.timestamp.toString)

    gson.toJson(adClickJsonObj)

  }



  def delayClicks(id: Int, interval:Int) = {

    val random = new Random()

    if (id % interval == 0) {
      val max = id -1
      val min = id - 9
      val clickId = random.nextInt((max - min) +1 ) + min
      val adClick = AdClick(clickId, new Date)

      val adClickJson = adClicktoJson(adClick)
      val adClickProducerRecord = new ProducerRecord[String, String](adClickTopic, adClickJson)
      producer.send(adClickProducerRecord)
      println("adClick: " + adClickJson)

      Thread.sleep(random.nextInt(3000 - 1000) + 1000)

    }
  }

  def publish(): Unit =  {

    val random = new Random()
    var i = 1
    // infinite loop
    while(true) {
      val id = i
      val timestamp = new Date
      //val timestamp = System.currentTimeMillis()
      val publisher = Publishers(random.nextInt(NumPublishers))
      val advertiser = Advertisers(random.nextInt(NumAdvertisers))
      val website = s"website_${random.nextInt(Constants.NumWebsites)}.com"
      val cookie = s"cookie_${random.nextInt(Constants.NumCookies)}"
      val geo = Geos(random.nextInt(Geos.size))
      val bid = math.abs(random.nextDouble()) % 1
      val adImpression = AdImpression(id, timestamp, publisher, advertiser, website, geo, bid, cookie)

      val adImpressionJson = adImpressiontoJson(adImpression)
      val adImpressionProducerRecord = new ProducerRecord[String, String](adImpressionTopic, adImpressionJson)
      producer.send(adImpressionProducerRecord)

      Thread.sleep(random.nextInt(3000 - 1000) + 1000)

      i = i + 1

      println("adImpression: " + adImpressionJson)

      delayClicks(i, 10)// Every 10th impression, one among the last 9 impression will be clicked

    }
    producer.close()

  }

  override def run() = {

    publish()
  }
}


object  AdvLogProducer {

  def main(args: Array[String]) {

   val adImpressionThread = new AdvLogProducer("localhost:9092", "adImpression", "adClick")
   adImpressionThread.start()

  }

}