import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}
import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.Random


import scala.io.Source

object from_csv_producer {

  def main(args: Array[String]): Unit = {


    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val rnd = new Random()
    //val producer = new KafkaProducer[String, Array[Byte]](props)
    val producer = new KafkaProducer[String, String](props)


    for (line <- Source.fromFile("/home/anis/cours_s2/spark/project_spark/3498_1146042_bundle_archive/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv").getLines().drop(1)) {

      val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("history", "history", line)

      producer.send(record)
      Thread.sleep(1000)
    }

    producer.close()
  }
}











