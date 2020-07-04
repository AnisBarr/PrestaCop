package alert

import org.apache.kafka.clients.producer._
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import Consumer.alert.Mailer.sendMail

import scala.collection.JavaConverters._

object consumer_alert {

  def main(args: Array[String]): Unit = {
    consumeFromKafka("alert_topic")
  }

  def consumeFromKafka(topic: String) = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "consumer-group-show-alert-from-drone")
    props.put("auto.offset.reset", "earliest")


    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))


    while (true) {

      val record = consumer.poll(100).asScala

      for (data <- record.iterator){

        val dt = data.value().split(",")
        val id_drone = dt(0)
        val latitude = dt(1)
        val longitude = dt(2)
        val timestamp = dt(3)
        val level_battrie = dt(4)

        val text = "information drone :" +
          "id drone : " + id_drone +
          " location ( "+ latitude+","+longitude+" ) " +
          " level_battrie : " + level_battrie

        sendMail(text, "Alerte drone "+id_drone+" Need humain action. Time: "+ timestamp)

      }

    }

  }

}

