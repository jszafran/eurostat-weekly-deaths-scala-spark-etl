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

  val metaCol      = rawDF.columns(0)
  val yearWeekCols = rawDF.columns.slice(1, rawDF.columns.size).toList

  val withMetaDF = transforms.extractMetadataDF(rawDF, metaCol = metaCol)

  val metaCols  = List("age", "sex", "country")
  val stackedDF = transforms.stackYearWeekData(withMetaDF, toStackCols = yearWeekCols, remainingCols = metaCols)

  println(stackedDF.show(5))
  println(s"Count after stack: ${stackedDF.count()}")
}
