package dev.jszafran.eurostat.weekly.deaths

import org.apache.spark.sql.{functions => F}
import dev.jszafran.eurostat.weekly.deaths.sqlUtils._

object Main extends App with SparkSessionWrapper {
  println("Hello from Eurostat Weekly Deaths Spark ETL")
  val rawDF = spark.read
    .option("inferschema", "false")
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("data/eurostat_weekly_deaths.tsv")

  val withMetaDF = transforms.extractMetadataDF(rawDF)

  val stackedDF = transforms.stackYearWeekData(withMetaDF)

  val yearWeekExtractedDF = transforms.extractYearWeekData(stackedDF)
  val deathsParsedDF      = transforms.parseDeaths(yearWeekExtractedDF)

  deathsParsedDF.write
    .partitionBy("country", "year")
    .parquet("results/eurostat.parquet")

}
