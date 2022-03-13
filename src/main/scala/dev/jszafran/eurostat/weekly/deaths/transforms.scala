package dev.jszafran.eurostat.weekly.deaths

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import dev.jszafran.eurostat.weekly.deaths.functions.{parseDeathsCol}
import dev.jszafran.eurostat.weekly.deaths.sqlUtils.{generateStackExpr}

object transforms {
  def extractMetadata()(df: DataFrame): DataFrame = {
    val metaCol      = df.columns(0)
    val splitPattern = ","
    df
      .withColumn("age", split(col(metaCol), splitPattern).getItem(0))
      .withColumn("sex", split(col(metaCol), splitPattern).getItem(1))
      .withColumn("country", split(col(metaCol), splitPattern).getItem(3))
      .drop(metaCol)
  }

  def stackYearWeekData()(df: DataFrame): DataFrame = {
    val metaCols    = List("age", "sex", "country")
    val toStackCols = df.columns.toSet.diff(metaCols.toSet).toList
    val cols        = metaCols.map(col) ::: List(expr(generateStackExpr("(yearweek, deaths)", toStackCols: _*)))
    df.select(cols: _*)
  }

  def extractYearWeekData()(df: DataFrame): DataFrame = {
    df
      .withColumn("year", split(col("yearweek"), "W").getItem(0).cast("Int"))
      .withColumn("week", split(col("yearweek"), "W").getItem(1).cast("Int"))
      .drop("yearweek")
  }

  def parseDeaths()(df: DataFrame): DataFrame = {
    df.withColumn("deaths", parseDeathsCol(col("deaths")))
  }
}
