import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object Main extends App {

  val spark = SparkSession.builder.master("local[*]")
    .appName("SparkApp2")
    .getOrCreate()
  val sc = spark.sparkContext

  val hdfsURI = "hdfs://localhost:9000"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsURI)
  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val input = "hdfs://localhost:9000/input2/nmea_aegean.logs"

  val outputPath1 = "/out1"
  val outputPath2 = "/out2"
  val outputPath3 = "/out3"
  val outputPath4 = "/out4"
  val outputPath5 = "/out5"

  val newNames = Seq("date", "station", "vesselID", "longitude", "latitude" , "SOG" , "COG" , "heading" , "status")

  val df = spark.read.option("header" , "true").csv(input).toDF(newNames: _*) // read the txt as a dataframe using read.csv and dropping the header

  val filteredDF = df.withColumn("date" , col("date").substr(0,10)) // isolate the date from timestamp

  val TrackedPositions = filteredDF.groupBy(
    col("date") , col("station")
  ).count()
    .groupBy(
    col("station")
  ).avg()                                                            // get the avg tracked vessel positions for each station per day

  val trackedPositionsAvg = TrackedPositions.groupBy().avg()         // get final result

  val TrackedPositionsRDD = trackedPositionsAvg.rdd

  trackedPositionsAvg.show()

  println("Number of tracked vessel positions per station per day: " + TrackedPositionsRDD.first().mkString(""))

  hdfs.delete(new Path(outputPath1), true)
  TrackedPositionsRDD.saveAsTextFile(outputPath1)

  val maxVesselID = filteredDF.groupBy(
    col("vesselID").as("VesselID with max positions tracked")
  ).agg(count(col("VesselID")).as("Positions Tracked"))
    .orderBy(col("Positions Tracked").desc).limit(1)

  val q2 = maxVesselID.select("VesselID with max positions tracked")

  q2.show()

  hdfs.delete(new Path(outputPath2), true)
  q2.rdd.saveAsTextFile(outputPath2)

  val temp1 = filteredDF.groupBy(
    col("vesselID") , col("date") , col("station")
  ).agg(
    avg(col("SOG")).as("SOG")
  ).filter(col("station") === 8006)

  val names2 = Seq("vesselID2", "date2", "station2", "SOG2")

  val temp2 = filteredDF.groupBy(
    col("vesselID") , col("date") , col("station")
  ).agg(
    avg(col("SOG")).as("SOG")
  ).filter(col("station") === 10003).toDF(names2: _*)

  val q3 = temp1.join(temp2 , temp1("vesselID") === temp2("vesselID2") && temp1("date") === temp2("date2"))
    .agg(avg((col("SOG") + col("SOG2"))/2).as("Average SOG"))

  q3.show()

  hdfs.delete(new Path(outputPath3), true)
  q3.rdd.saveAsTextFile(outputPath3)

  val q4 = filteredDF.groupBy( col("station") )
    .agg(
    avg(abs(col("heading") - col("COG"))).as("avg")
  ).groupBy().agg(avg(col("avg")).as("Average Abs(Heading – COG) per station"))

  q4.show()

  hdfs.delete(new Path(outputPath4), true)
  q4.rdd.saveAsTextFile(outputPath4)

  val q5 = filteredDF.groupBy(col("status"))
    .agg( count(col("status")).as("count"))
    .orderBy(col("count").desc).select(col("status").as("Top-3 Frequent Statuses")).limit(3)

  q5.show()

  hdfs.delete(new Path(outputPath5), true)
  q5.rdd.saveAsTextFile(outputPath5)

  spark.stop()

}