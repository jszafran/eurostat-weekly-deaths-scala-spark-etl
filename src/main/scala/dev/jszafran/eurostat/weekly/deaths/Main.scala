package dev.jszafran.eurostat.weekly.deaths

object Main extends App with SparkSessionWrapper {
  val rawDF = spark.read
    .option("inferschema", "false")
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("data/eurostat_weekly_deaths.tsv")

  val withMetaDF = transforms.extractMetadata(rawDF)

  val stackedDF = transforms.stackYearWeekData(withMetaDF)

  val yearWeekExtractedDF = transforms.extractYearWeekData(stackedDF)
  val deathsParsedDF      = transforms.parseDeaths(yearWeekExtractedDF)

  deathsParsedDF.write
    .partitionBy("country", "year")
    .parquet("results/eurostat.parquet")

}
