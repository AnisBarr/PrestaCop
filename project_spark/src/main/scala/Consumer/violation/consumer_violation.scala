import java.io.{BufferedOutputStream, FileOutputStream}
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import scala.collection.JavaConverters._

object consumer_violation {

  def main(args: Array[String]): Unit = {

    consumeFromKafka("image_violation")

  }

  def consumeFromKafka(topic: String) = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "consumer-group-save-image-violation")


    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)

    consumer.subscribe(util.Arrays.asList(topic))

    while (true) {

      val record = consumer.poll(1000).asScala

      val conf = new Configuration()
      conf.addResource(new Path("/home/anis/cours_s2/spark/project_spark/hadoop-2.10.0/etc/hadoop/core-site.xml"))
      val fs= FileSystem.get(conf)

      for (data <- record.iterator){
        println(data.key())
        val id = data.key()
        println(id)

        //hdfs local
        val output = fs.create(new Path("/storage/violation/image_violation/image_violation_id_"+id+".png"))
        output.write(data.value())
        output.close()


        //s3 aws
        /*val conf = new Configuration()
        conf.set("fs.s3a.access.key", "AKIAZYGYRIHTTXXSDCWG")
        conf.set("fs.s3a.secret.key", "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")
        conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

        val dest = new Path("s3a://sparkprojet/image"+id+".png")
        val fs = dest.getFileSystem(conf)
        val out = fs.create(dest, true)
        out.write(data.value())
        out.close()*/


        //local
        /*val bos = new BufferedOutputStream(new FileOutputStream("image"+id+".png"))
        bos.write(data.value())
        bos.close()*/
      }

    }

  }

}