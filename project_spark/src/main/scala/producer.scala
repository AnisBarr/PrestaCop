import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.Random



object Producer {

  def main(args: Array[String]): Unit = {

    writeToKafka("drone_topic")

  }

  def message(id: Int): String = {
    val rnd = new Random()
    val latitude = 40+ rnd.nextDouble()
    val longitude = -73 - rnd.nextDouble()
    val timestamp: Long = System.currentTimeMillis / 1000
    id+","+latitude +","+longitude+","+timestamp
  }


  def writeToKafka(topic: String): Unit = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    while (true){
      for (i <- 0 to 100) {
        var record = new ProducerRecord[String, String](topic, "key",message(i))
        producer.send(record)
      }
      Thread.sleep(1000)
    }

    producer.close()

  }

}


/*import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object ScalaProducerExample extends App {
  val events = 50
  val topic = "drone_topic"
  val brokers = "localhost:9092"
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  for (nEvents <- Range(0, events)) {
    val runtime = new Date().getTime()
    val ip = "192.168.2." + rnd.nextInt(255)
    val msg = runtime + "," + nEvents + ",www.example.com," + ip
    val data = new ProducerRecord[String, String](topic, ip, msg)

    //async
    //producer.send(data, (m,e) => {})
    //sync
    producer.send(data)
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}






import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerApp extends App {

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
         "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
         "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "drone_topic"
  try {
    for (i <- 0 to 15) {
      val record = new ProducerRecord[String, String](topic, i.toString, "My Site is sparkbyexamples.com " + i)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}



import org.apache.spark.sql.SparkSession

object KafkaProduceJson {

  def main(args:Array[String]): Unit ={


    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq((1,"James ","","Smith",2018,1,"M",3000),
      (2,"Michael ","Rose","",2010,3,"M",4000),
      (3,"Robert ","","Williams",2010,3,"M",4000),
      (4,"Maria ","Anne","Jones",2005,5,"F",4000),
      (5,"Jen","Mary","Brown",2010,7,"",-1)
    )

    val columns = Seq("id","firstname","middlename","lastname","dob_year","dob_month","gender","salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    val ds = df.toJSON
    ds.printSchema()

    val query = ds
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "drone_topic")
      .start()

  }
}
*/