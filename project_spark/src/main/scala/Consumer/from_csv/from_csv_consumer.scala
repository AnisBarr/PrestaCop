package Consumer.from_csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object from_csv_consumer {

  def main(args: Array[String]): Unit = {


    case class Message(id_drone : Int, cord_latitude: Double, cord_longitude: Double, unix_time: Long,  level_batterie : Int, temperature : Int)

    val spark:SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config("spark.sql.streaming.checkpointLocation", "./checkpoint")
      .master("local")
      .appName("from_csv_consumer")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "history")
      .option("group.id", "consumer-group-178")
      .option("auto.offset.reset", "earliest")
      .load()

    val list =  Seq("Summons Number","Plate ID","Registration State","Plate Type","Issue Date","Violation Code","Vehicle Body Type","Vehicle Make","Issuing Agency","Street Code1","Street Code2","Street Code3","Vehicle Expiration Date"
      ,"Violation Location","Violation Precinct","Issuer Precinct","Issuer Code","Issuer Command","Issuer Squad","Violation Time","Time First Observed","Violation County"
      ,"Violation In Front Of Or Opposite","House Number","Street Name","Intersecting Street","Date First Observed","Law Section","Sub Division","Violation Legal Code"
      ,"Days Parking In Effect","From Hours In Effect","To Hours In Effect","Vehicle Color","Unregistered Vehicle?","Vehicle Year","Meter Number"
      ,"Feet From Curb","Violation Post Code","Violation Description","No Standing or Stopping Violation","Hydrant Violation","Double Parking Violation","Latitude"
      ,"Longitude","Community Board","Community Council" ,"Census Tract","BIN","BBL","NTA")



    val test = list.zipWithIndex
      .foldLeft(df.withColumn("new_column", split(df("value"), ",")))
      { case (df, (c, i)) => df.withColumn(c,   $"new_column".getItem(i).as(list(i))) }


    val result= test.drop("key").drop("value").drop("topic").drop("offset")
        .drop("timestamp").drop("timestampType").drop("new_column").drop("partition")


    result.writeStream.format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

/*    result.writeStream
      .format("parquet")
      .option("path", "hdfs://localhost:9000/storage/history/")
      .start()
      .awaitTermination()
*/
  }
}