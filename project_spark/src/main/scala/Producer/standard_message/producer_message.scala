import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.Random
import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import play.api.libs.json._
import play.api.libs.functional.syntax._


import java.io.{ByteArrayOutputStream, ObjectOutputStream}


object producer_message {

  def main(args: Array[String]): Unit = {

    writeToKafka("drone_topic")

  }

  case class Message(id_drone : Int, cord_latitude: Double, cord_longitude: Double, unix_time: Long,  level_batterie : Int, temperature : Int)

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value).asInstanceOf[Message]
    oos.close()
    stream.toByteArray
  }

  def message(id: Int,rnd : Random ) = {

    val latitude = 40+ rnd.nextDouble()
    val longitude = -73 - rnd.nextDouble()
    val timestamp: Long = System.currentTimeMillis / 1000
    val level_battrie = 50 + rnd.nextInt(50) - rnd.nextInt(30)
    val temperature = 20 + rnd.nextInt(10) - rnd.nextInt(15)

    //Message(id,latitude,longitude,timestamp,level_battrie,temperature)
    id+","+latitude+","+longitude+","+timestamp+","+level_battrie+","+","+temperature
  }

  def class_to_json_string (message : Message) : String ={

    implicit val formats = DefaultFormats
    val jsonString = write(message)
    println(jsonString)
    jsonString
  }





  def writeToKafka(topic: String): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //props.put("value.serializer", org.apache.kafka.common.serialization.)
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val rnd = new Random()
    //val producer = new KafkaProducer[String, Array[Byte]](props)
    val producer = new KafkaProducer[String, String](props)

    while (true){
      for (i <- 0 to 100) {

        // val record = new ProducerRecord[String, Array[Byte]](topic, "message", serialise(message(i,rnd)))
        val record = new ProducerRecord[String, String](topic, "message", message(i,rnd))

        producer.send(record)
      }
      Thread.sleep(1000)
    }

    producer.close()

  }

}



