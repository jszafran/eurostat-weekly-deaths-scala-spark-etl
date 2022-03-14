package dev.jszafran.eurostat.weekly.deaths

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object functions {

  def parseDeaths(col: Column) = {
    when(col.contains(":"), lit(null))
      .otherwise(regexp_replace(col, "p", ""))
      .cast("Int")
  }
}
