package com.scaliot.data

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataLoader(spark: SparkSession) {
  def loadCSV(filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }
}
