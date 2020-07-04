package alert

import org.apache.kafka.clients.producer._
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import scala.collection.JavaConverters._

object consumer_prodrucer_alert {

  def main(args: Array[String]): Unit = {
    consumeFromKafka("drone_topic")
  }

  def consumeFromKafka(topic: String) = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "consumer-group-show-alert")
    props.put("auto.offset.reset", "earliest")

    val props_alert = new Properties()
    props_alert.put("bootstrap.servers", "localhost:9092")
    props_alert.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props_alert.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))

    val producer_alert = new KafkaProducer[String, String](props_alert)


    while (true) {

      val record = consumer.poll(100).asScala

      for (data <- record.iterator){
        val dt = data.value().split(",")
        if (dt.length >6) {

          val id_violation = dt(6)
          println(id_violation)

          if (id_violation == "999") {
            val record = new ProducerRecord[String, String]("alert_topic","alert" ,data.value())
            producer_alert.send(record)
            print(data.value())

          }

        }

      }

    }

  }

}

