# eurostat-weekly-deaths

ETL for transforming Eurostat Weekly Deaths data with Scala & Spark.

Template created with `MrPowers/spark-sbt.g8` giter8 template.

## Dataset description

Dataset contains weekly deaths data reported by countries within EU. Weeks are numbered according to ISO8601 (single year can have either 52 or 53 weeks).

Original data is in TSV format and is of a "wide" shape - each column (except for first one with metadata) represents a single week of a particular year (i.e. week 1 of 2020). Dataset has 1156 column (and this grows as new weekly data is added). Data is reported for various demographics (age, sex, country) and each row represents a full history for a different demographic combination (i.e. history of weekly deaths reported for all male in Italy for age between 10 and 14 years old).

Spark application transforms data into a form that is more suitable for analytics - "narrow" shape where columns amount is fixed and dataset will grow "vertically" as new datapoints arrive.

```
+-----+---+------+----+-------+----+
|  age|sex|deaths|week|country|year|
+-----+---+------+----+-------+----+
|TOTAL|  F|  6603|  32|     IT|2015|
|TOTAL|  F|  6823|  15|     IT|2015|
|TOTAL|  F|  7345|  11|     IT|2015|
|TOTAL|  F|  5882|  42|     IT|2015|
|TOTAL|  F|  5940|  38|     IT|2015|
|TOTAL|  F|  5607|  34|     IT|2015|
|TOTAL|  F|  7118|  29|     IT|2015|
|TOTAL|  F|  7065|  12|     IT|2015|
|TOTAL|  F|  5282|  25|     IT|2015|
|TOTAL|  F|  8242|   7|     IT|2015|
|TOTAL|  F|  6226|  33|     IT|2015|
|TOTAL|  F|  6131|  43|     IT|2015|
|TOTAL|  F|  5922|  47|     IT|2015|
|TOTAL|  F|  8255|   3|     IT|2015|
|TOTAL|  F|  7585|   9|     IT|2015|
|TOTAL|  F|  8386|   2|     IT|2015|
|TOTAL|  F|  6036|  20|     IT|2015|
|TOTAL|  F|  5760|  21|     IT|2015|
|TOTAL|  F|  5479|  39|     IT|2015|
|TOTAL|  F|  5725|  35|     IT|2015|
+-----+---+------+----+-------+----+
```

Transformed data is partitioned by year & country and persisted in a **parquet** format.

## Tests

Tests can be ran with

```
sbt test
```
