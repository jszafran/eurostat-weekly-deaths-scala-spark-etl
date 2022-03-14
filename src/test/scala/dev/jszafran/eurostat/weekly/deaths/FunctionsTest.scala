package dev.jszafran.eurostat.weekly.deaths

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.Row

class FunctionsSpec extends FunSpec with SparkSessionTestWrapper with ColumnComparer {

  import spark.implicits._

  describe("parseDeaths") {
    it("parses string representation of weekly deaths value; if data not present, it returns null") {
      val data = Seq(
        ("123", 123),
        ("11", 11),
        ("25 p", 25)
        // (": ", null)  TODO: figure out how to test null value
      )

      val df = data
        .toDF("weekly_deaths", "expected")
        .withColumn("actual", functions.parseDeaths(col("weekly_deaths")))

      assertColumnEquality(df, "actual", "expected")
    }
  }

}
