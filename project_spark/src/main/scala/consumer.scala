import java.io.{BufferedOutputStream, FileOutputStream}
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import scala.collection.JavaConverters._

object Consumer {

  def main(args: Array[String]): Unit = {

    consumeFromKafka("drone_topic")

  }

  def consumeFromKafka(topic: String) = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("auto.offset.reset", "latest")

    props.put("group.id", "consumer-group")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList(topic))

    while (true) {

      val record = consumer.poll(1000).asScala

      for (data <- record.iterator){
        println(data.value())
        val bos = new BufferedOutputStream(new FileOutputStream("filename"data.value()))
        bos.write(byteArray)
        bos.close()
      }

    }

  }

}

/*
import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.consumer.KafkaStream
import org.apache.kafka.utils.Logging

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

class ScalaConsumerExample(val brokers: String,
                           val groupId: String,
                           val topic: String) extends Logging {

  val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.close();
    if (executor != null)
      executor.shutdown();
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(    new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)

          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}

object ScalaConsumerExample extends App {
  val example = new ScalaConsumerExample("localhost:9092", "group1", "drone_topic")
  example.run()
}




import java.util.{Collections, Properties}
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object consumer extends App {

  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("dorne_topic")
  try {
    consumer.subscribe(topics.asJava)
    //consumer.subscribe(Collections.singletonList("topic_partition"))
    //consumer.subscribe(Pattern.compile("topic_partition"))
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value() +
          ", Offset: " + record.offset() + ", Partition: " + record.partition())
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
}*/