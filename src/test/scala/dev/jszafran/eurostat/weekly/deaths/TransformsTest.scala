package dev.jszafran.eurostat.weekly.deaths

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class TransformsSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {
  import spark.implicits._
  describe("withEurostatDataTransformed") {
    it("integration test for Eurostat transformations pipeline") {
      val inputDF = Seq(
        ("TOTAL,F,NR,AD", "25 p", "24 p", "25", "1"),
        ("Y10-14,T,NR,PL", "100 p", "101 p", "999", "1"),
        ("Y20-24,M,NR,DE", "2000", "3000 p", "2500", "1 p")
      ).toDF("meta", "2020W01", "2019W10", "2021W15", "2021W99")

      val actualDF = inputDF.transform(transforms.withEurostatDataTransformed())

      val colNames    = Seq("age", "sex", "country", "year", "week", "deaths")
      val colsOrdered = colNames.map(col)
      val expectedDF = Seq(
        ("TOTAL", "F", "AD", 2020, 1, 25),
        ("TOTAL", "F", "AD", 2019, 10, 24),
        ("TOTAL", "F", "AD", 2021, 15, 25),
        ("Y10-14", "T", "PL", 2020, 1, 100),
        ("Y10-14", "T", "PL", 2019, 10, 101),
        ("Y10-14", "T", "PL", 2021, 15, 999),
        ("Y20-24", "M", "DE", 2020, 1, 2000),
        ("Y20-24", "M", "DE", 2019, 10, 3000),
        ("Y20-24", "M", "DE", 2021, 15, 2500)
      ).toDF(colNames: _*)

      assert(actualDF.count() === expectedDF.count())
      assertSmallDataFrameEquality(
        actualDF.select(colsOrdered: _*),
        expectedDF.select(colsOrdered: _*),
        ignoreNullable = true,
        orderedComparison = false
      )
    }
  }
}
