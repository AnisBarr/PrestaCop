import java.awt.image.BufferedImage
import java.util.Properties
import java.nio.file.{Files, Paths}


import org.apache.kafka.clients.producer._

import scala.util.Random


object producer_violation {


  def main(args: Array[String]): Unit = {

    writeToKafka("drone_topic")

  }

  def writeToKafka(topic: String): Unit = {

    val props_image = new Properties()

    props_image.put("bootstrap.servers", "localhost:9092")
    props_image.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props_image.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer_image = new KafkaProducer[String, Array[Byte]](props_image)
    val producer = new KafkaProducer[String, String](props)
    val rnd = new Random()
    var id =0


    def message(id_drone: Int,rnd : Random ,id_image :Int ) : String= {

      val latitude = 40+ rnd.nextDouble()
      val longitude = -73 - rnd.nextDouble()
      val timestamp: Long = System.currentTimeMillis / 1000
      val level_battrie = 50 + rnd.nextInt(50) - rnd.nextInt(30)
      val temperature = 20 + rnd.nextInt(10) - rnd.nextInt(15)
      val id_violation = rnd.nextInt(100)

      //Message(id,latitude,longitude,timestamp,level_battrie,temperature)
      id_drone+","+latitude+","+longitude+","+timestamp+","+level_battrie+","+","+temperature+","+id_violation+","+id_image

    }


    while (true){
      id = id + 1
      val i =  rnd.nextInt(100)+1
      val drone_id = rnd.nextInt(100)

      val byteArray = Files.readAllBytes(Paths.get("src/main/resources/test"+i+".png"))

      val record = new ProducerRecord[String, String](topic,"message" ,message(drone_id,rnd,id))
      val rec = new ProducerRecord[String, Array[Byte]]("image_violation", id.toString,byteArray)

      producer.send(record)
      producer_image.send(rec)
      Thread.sleep(30000)

    }

    producer.close()

  }

}


