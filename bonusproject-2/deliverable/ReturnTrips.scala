import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

// (c) 2021 Thomas Neumann, Timo Kersten, Alexander Beischl, Maximilian Reif



object ReturnTrips {
  val earthRadius = 6371000


  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._

    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", true)
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize",100000)
    spark.conf.set("spark.sql.shuffle.partitions",3000)
    spark.conf.set("spark.sql.cbo.enabled", true)
    spark.conf.set("spark.sql.adaptive.enabled",true)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",true)
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",true)

    


    def makeDistExpr(lat1: Column, lon1: Column, lat2: Column, lon2: Column): Column = {
      val hav = pow(sin(abs(lat2 - lat1) / 2), 2) + cos(lat1) * cos(lat2) * pow(sin(abs(lon1 - lon2) / 2), 2)
      lit(earthRadius * 2) * asin(sqrt(hav))

  }

    val denom = 1.5 * dist/earthRadius
    val tripsbucks = trips.where($"pickup_longitude" =!= 0 && 
    $"pickup_latitude" =!= 0 && 
    $"dropoff_longitude" =!= 0 
    && $"dropoff_latitude" =!= 0).withColumn("plat_bucket", floor(toRadians($"pickup_latitude")/denom))
    .withColumn("dlat_bucket", floor(toRadians($"dropoff_latitude")/denom))
    .withColumn("ptime_bucket", floor(unix_timestamp($"tpep_pickup_datetime")/28800))
    .withColumn("dtime_bucket", floor(unix_timestamp($"tpep_dropoff_datetime")/28800)).select(
      "plat_bucket",
      "dlat_bucket",
      "ptime_bucket",
      "dtime_bucket",
      "pickup_latitude",
      "pickup_longitude",
      "dropoff_longitude",
      "dropoff_latitude",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime"
    )



    

    val tripsBuckNeighbors = tripsbucks
    .withColumn("dlat_bucket", explode(array($"dlat_bucket", $"dlat_bucket"-1, $"dlat_bucket"+1)))
    .withColumn("ptime_bucket", explode(array($"ptime_bucket", $"ptime_bucket"-1)))
    .withColumn("plat_bucket", explode(array($"plat_bucket", $"plat_bucket"-1, $"plat_bucket"+1)))

    val selected_trips = tripsBuckNeighbors.as("b").join(tripsbucks.as("a"), 
    (makeDistExpr(toRadians($"a.dropoff_latitude"), toRadians($"a.dropoff_longitude"), toRadians($"b.pickup_latitude"), 
    toRadians($"b.pickup_longitude")) < dist)
     && 
     (makeDistExpr(toRadians($"b.dropoff_latitude"), toRadians($"b.dropoff_longitude"), toRadians($"a.pickup_latitude"), 
     toRadians($"a.pickup_longitude"))< dist)
      &&  (unix_timestamp($"a.tpep_dropoff_datetime") < unix_timestamp($"b.tpep_pickup_datetime"))
      && (unix_timestamp($"a.tpep_dropoff_datetime")+ 28800 > unix_timestamp($"b.tpep_pickup_datetime"))
      && ($"b.ptime_bucket" === $"a.dtime_bucket") && 
      ($"a.plat_bucket" === $"b.dlat_bucket") && 
      ($"a.dlat_bucket" === $"b.plat_bucket") 
      )

    selected_trips
  }
}
