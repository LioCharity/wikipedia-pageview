package com.lsouoppekam.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object Writer {

  /**
    * The method writes the dataframe as a csv in a single file
    * @param df Data frame to be saved
    * @param outputDir output directory
    * @param spark SparkSession
    */
  def saveAsCSV(df: DataFrame, outputDir: String)(implicit
      spark: SparkSession
  ): Unit = {
    df.repartition(1).write.mode("overwrite").csv(outputDir)
  }
}
