package dev.jszafran.eurostat.weekly.deaths

import transforms._

object Main extends App with SparkSessionWrapper {
  // read raw input data
  val rawDF = spark.read
    .option("inferschema", "false")
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("data/eurostat_weekly_deaths.tsv")

  // transform data
  val transformedDF = rawDF
    .transform(extractMetadata())
    .transform(stackYearWeekData())
    .transform(extractYearWeekData())
    .transform(filterOutBadWeeksData())
    .transform(parseDeaths())

  // persist transformed data
  transformedDF.write
    .partitionBy("country", "year")
    .parquet("results/eurostat.parquet")
}
