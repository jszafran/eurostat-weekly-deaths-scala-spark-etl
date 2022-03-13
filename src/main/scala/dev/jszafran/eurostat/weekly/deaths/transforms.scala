package dev.jszafran.eurostat.weekly.deaths

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import dev.jszafran.eurostat.weekly.deaths.sqlUtils._

object transforms {
  def extractMetadataDF(df: DataFrame, metaCol: String): DataFrame = {
    val splitPattern = ","
    df
      .withColumn("age", split(col(metaCol), splitPattern).getItem(1))
      .withColumn("sex", split(col(metaCol), splitPattern).getItem(2))
      .withColumn("country", split(col(metaCol), splitPattern).getItem(4))
      .drop(metaCol)
  }

  def stackYearWeekData(df: DataFrame, toStackCols: List[String], remainingCols: List[String]): DataFrame = {
    val cols = remainingCols.map(col) ::: List(expr(generateStackExpr("(yearweek, deaths)", toStackCols: _*)))
    df.select(cols: _*)
  }
}
