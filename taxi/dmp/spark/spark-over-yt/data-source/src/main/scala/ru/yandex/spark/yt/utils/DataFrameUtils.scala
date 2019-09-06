package ru.yandex.spark.yt.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._

object DataFrameUtils {
  implicit class UtilsDataFrame(df: DataFrame) {
    private val shufflePartitions = df.sparkSession.sqlContext.getConf("spark.sql.shuffle.partitions").toInt

    def top(groupBy: Seq[String], topBy: Seq[String], partitions: Int = shufflePartitions): DataFrame = {
      val sortCols = groupBy ++ topBy
      df
        .repartition(partitions, groupBy.map(col):_*)
        .sortWithinPartitions(sortCols.head, sortCols.tail:_*)
        .mapPartitions { rows =>
          var currentKey = Option.empty[Seq[String]]
          rows.flatMap { row =>
            val key = groupBy.map(row.getAs[String])
            if (!currentKey.contains(key)) {
              currentKey = Some(key)
              Some(row)
            } else {
              None
            }
          }
        }(RowEncoder(df.schema))
    }
  }

  def getDataFrameTop(df: DataFrame, groupBy: java.util.ArrayList[String], topBy: java.util.ArrayList[String], partitions: java.lang.Integer): DataFrame = {
    import scala.collection.JavaConverters._
    df.top(groupBy.asScala, topBy.asScala, partitions)
  }

}
