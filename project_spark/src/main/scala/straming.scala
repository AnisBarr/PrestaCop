import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object straming {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "drone_topic")
      .option("group.id", "consumer-group")
      .load()

    df.printSchema()

    //val groupCount = df.select(split(df("value"),","))
    val tmp_df = df.withColumn("_tmp", split($"value", ","))
      .select($"_tmp".getItem(0).as("id_drone"),
        $"_tmp".getItem(1).as("lat"),
        $"_tmp".getItem(2).as("log"),
        $"_tmp".getItem(3).as("time")
      )


    tmp_df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}