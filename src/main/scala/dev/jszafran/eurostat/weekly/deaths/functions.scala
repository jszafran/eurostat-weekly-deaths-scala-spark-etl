package dev.jszafran.eurostat.weekly.deaths

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object functions {

  def isEven(col: Column): Column = {
    col % 2 === lit(0)
  }

  def parseDeaths(col: Column) = {
    when(col.contains(":"), lit(0))
      .otherwise(regexp_replace(col, ":", ""))
      .cast("Int")
  }
}
