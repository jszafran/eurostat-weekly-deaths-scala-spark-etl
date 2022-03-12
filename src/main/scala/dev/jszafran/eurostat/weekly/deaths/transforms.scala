package dev.jszafran.eurostat.weekly.deaths

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object transforms {

  def addIdColumn(df: DataFrame): DataFrame = {
    df.withColumn("id", monotonically_increasing_id())
  }

}
