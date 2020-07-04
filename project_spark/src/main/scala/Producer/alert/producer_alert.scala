import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.Random

import java.io.{ByteArrayOutputStream, ObjectOutputStream}


object producer_alert{

  def main(args: Array[String]): Unit = {

    writeToKafka("drone_topic")

  }

  def message(id_drone: Int,rnd : Random  ) : String= {

    val latitude = 40+ rnd.nextDouble()
    val longitude = -73 - rnd.nextDouble()
    val timestamp: Long = System.currentTimeMillis / 1000
    val level_battrie = 50 + rnd.nextInt(50) - rnd.nextInt(30)
    val temperature = 20 + rnd.nextInt(10) - rnd.nextInt(15)
    val id_violation = 999

    //Message(id,latitude,longitude,timestamp,level_battrie,temperature)
    id_drone+","+latitude+","+longitude+","+timestamp+","+level_battrie+","+temperature+","+id_violation

  }


  def writeToKafka(topic: String): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val rnd = new Random()
    //val producer = new KafkaProducer[String, Array[Byte]](props)
    val producer = new KafkaProducer[String, String](props)

    while (true){
        // val record = new ProducerRecord[String, Array[Byte]](topic, "message", serialise(message(i,rnd)))
        val id_drone =  rnd.nextInt(100)+1
        val record = new ProducerRecord[String, String](topic, "alert", message(id_drone,rnd))
        producer.send(record)

      Thread.sleep(100000)
    }

    producer.close()

  }

}



