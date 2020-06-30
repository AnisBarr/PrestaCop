import java.awt.image.BufferedImage
import java.util.Properties
import java.nio.file.{Files, Paths}


import org.apache.kafka.clients.producer._

import scala.util.Random


object Producer_violation {


  def main(args: Array[String]): Unit = {

    writeToKafka("drone_topic")

  }

  def writeToKafka(topic: String): Unit = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[String, Array[Byte]](props)
    val byteArray = Files.readAllBytes(Paths.get("src/main/resources/test.png"))
    while (true){

      for (i <- 0 to 100) {
        var record = new ProducerRecord[String, Array[Byte]](topic, i.toString,byteArray)
        producer.send(record)
      }
      Thread.sleep(10000)
    }

    producer.close()

  }

}


/*


import java.util.Properties

import org.apache.kafka.clients.producer._
import scala.util.Random
import java.time.LocalDateTime


object producer_violation {

  def main(args: Array[String]): Unit = {

    writeToKafka("drone_topic")

  }

  def message(id: Int): String = {
    val rnd = new Random()
    val latitude = 40+ rnd.nextDouble()
    val longitude = -73 - rnd.nextDouble()
    val timestamp: Long = System.currentTimeMillis / 1000

val data = Array[Byte](10, -32, 17, 22)



  }

  def writeToKafka(topic: String): Unit = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    for (i <- 0 to 15) {
      var record = new ProducerRecord[String, String](topic, "key",message())
      producer.send(record)
    }
    producer.close()

  }

}
*/