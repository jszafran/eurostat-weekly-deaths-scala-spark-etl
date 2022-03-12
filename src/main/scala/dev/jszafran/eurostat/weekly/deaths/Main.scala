package dev.jszafran.eurostat.weekly.deaths

object Main extends App with SparkSessionWrapper {
  println("Hello from Eurostat Weekly Deaths Spark ETL")
  val df_raw = spark.read
    .option("inferSchema", "false")
    .option("header", "true")
    .csv("data/eurostat_weekly_deaths.tsv")
}
