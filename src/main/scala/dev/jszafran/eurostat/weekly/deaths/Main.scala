package dev.jszafran.eurostat.weekly.deaths

import org.apache.spark.sql.{functions => F}

object Main extends App with SparkSessionWrapper {
  println("Hello from Eurostat Weekly Deaths Spark ETL")
  val rawDF = spark.read
    .option("inferschema", "false")
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("data/eurostat_weekly_deaths.tsv")

  val metaCol      = rawDF.columns(0)
  val weeksDataCol = rawDF.columns.slice(1, rawDF.columns.size)

  // add id column
  val withIdDF = transforms.addIdColumn(rawDF)
}
