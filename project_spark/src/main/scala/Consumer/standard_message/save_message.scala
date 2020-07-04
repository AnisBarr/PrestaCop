import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object save_message {

  def main(args: Array[String]): Unit = {


    case class Message(id_drone : Int, cord_latitude: Double, cord_longitude: Double, unix_time: Long,  level_batterie : Int, temperature : Int)

    val spark:SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config("spark.sql.streaming.checkpointLocation", "./checkpoint")
      .master("local")
      .appName("save_message")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "drone_topic")
      .option("group.id", "consumer-group-1")
      .option("auto.offset.reset", "earliest")
      .load()

    df.printSchema()


    //val groupCount = df.select(split(df("value"),","))
    val tmp_df = df.withColumn("_tmp", split($"value", ","))
      .select($"_tmp".getItem(0).as("id_drone"),
        $"_tmp".getItem(1).as("cord_latitude"),
        $"_tmp".getItem(2).as("cord_longitude"),
        $"_tmp".getItem(3).as("unix_time"),
        $"_tmp".getItem(4).as("level_batterie"),
        $"_tmp".getItem(6).as("temperature"),
        $"_tmp".getItem(7).as("id_violation"),
        $"_tmp".getItem(8).as("id_image_violation")


        //id_drone : Int, cord_latitude: Double, cord_longitude: Double, unix_time: Long,  level_batterie : Int,status_drone: String, temperature : Int)
      )


    tmp_df.writeStream
      .format("parquet")
      .option("path", "hdfs://localhost:9000/storage/message/")
      .start()
      .awaitTermination()

    tmp_df.writeStream.format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}