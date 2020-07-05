package Analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.functions._


object analyse {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local")
      .appName("save_message")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .parquet("hdfs://localhost:9000/storage/message/part*")
    println("nomber of row : "+df.count())

    // moyene niveau baterie
    val df_moyen_batterie = df.select(avg("level_batterie"))
    df_moyen_batterie.show()

    // l'infraction la plus courante
    val df_most_violation = df.filter(col("id_violation").isNotNull).groupBy("id_violation").count().sort(desc("count"))
    df_most_violation.show(10)

    // moyene niveau baterie
    val df_moyen_temperature = df.select(avg("temperature"))
    df_moyen_temperature.show()

    // localisation des point de violation
    val df_most_location = df.filter(col("id_violation").isNotNull).groupBy("cord_latitude","cord_longitude").count().sort(desc("count"))
    df_most_location.show(10)

    // les violation en fonction des drone
    val df_drone_violation = df.filter(col("id_violation").isNotNull).groupBy("id_drone","id_violation").count().sort(desc("count"))
    df_drone_violation.show(10)

    // les drone qui envoie le plus d'alert
    /*val df_drone_alert = df.filter(col("id_violation" ).isNotNull).filter( col("id_violation") = 999).groupBy("id_drone","id_violation").count().sort(desc("count"))
    df_drone_alert.show(10)*/

  }
}