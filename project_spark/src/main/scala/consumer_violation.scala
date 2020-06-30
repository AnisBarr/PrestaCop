import java.io.{BufferedOutputStream, FileOutputStream}
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import scala.collection.JavaConverters._

object consumer_violation {

  def main(args: Array[String]): Unit = {

    consumeFromKafka("drone_topic")

  }

  def consumeFromKafka(topic: String) = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    props.put("auto.offset.reset", "latest")

    props.put("group.id", "consumer-group")

    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)

    consumer.subscribe(util.Arrays.asList(topic))
    var int = 0
    while (true) {

      val record = consumer.poll(1000).asScala

      for (data <- record.iterator){
        //println(data.value())
        int = int + 1
        val bos = new BufferedOutputStream(new FileOutputStream("image"+int+".png"))
        bos.write(data.value())
        bos.close()
      }

    }

  }

}