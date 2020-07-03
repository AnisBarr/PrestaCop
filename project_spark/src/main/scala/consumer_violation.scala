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
      val conf = new Configuration()
      conf.addResource(new Path("/home/anis/cours_s2/spark/project_spark/hadoop-2.10.0/etc/hadoop/core-site.xml"))
      val fs= FileSystem.get(conf)
      //conf.set("fs.default.name", "hdfs://localhost:9000")

      // pickup config files off classpath
      // Configuration conf = new Configuration()
      // explicitely add other config files
      // PASS A PATH NOT A STRING!
      // conf.addResource(new Path("/home/hadoop/conf/core-site.xml"));
      // FileSystem fs = FileSystem.get(conf); // load files and stuff below!

      for (data <- record.iterator){
        int = int + 1
        println(data.value())

        //hdfs local
        val output = fs.create(new Path("/image"+int+".png"))
        output.write(data.value())
        output.close()


        //s3 aws
        /*val conf = new Configuration()
        conf.set("fs.s3a.access.key", "AKIAZYGYRIHTTXXSDCWG")
        conf.set("fs.s3a.secret.key", "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")
        conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

        val dest = new Path("s3a://sparkprojet/image"+int+".png")
        val fs = dest.getFileSystem(conf)
        val out = fs.create(dest, true)
        out.write(data.value())
        out.close()*/


        //local
        /*val bos = new BufferedOutputStream(new FileOutputStream("image"+int+".png"))
        bos.write(data.value())
        bos.close()*/
      }

    }

  }

}