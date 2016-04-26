package com.liny.spark.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

object MysqlMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Desensitize")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    Class.forName("oracle.jdbc.OracleDriver").newInstance();
    val df = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false")
      .load("F:\\公司文档\\精诚瑞宝\\广发项目交接资料\\01_广发\\01_data\\sample-data.csv")

    val props = new java.util.Properties()
    val data = sc.wholeTextFiles("G:\\test\\data\\*", 3)
    val lineDat = data.map {
      case (filename, content) => {
        val lines = content.split("\r\n|\n") //分割行
        lines.toVector
      }
    }

    import sqlc.implicits._
    val dat = lineDat.flatMap(lines => {
      lines.map { line =>
        {
          val values = line.split(",").toList
          Row.fromSeq(values)
        }
      }
    })

    val outDriverUrl = "jdbc:mysql://localhost:3306/stp?user=root&password=348696025&zeroDateTimeBehavior=convertToNull&amp;characterEncoding=utf-8"
    val outTableName = "GUANGFA"
    val df2 = sqlc.createDataFrame(dat, df.schema)
    df2.write.mode(SaveMode.Overwrite).jdbc(outDriverUrl, outTableName, props)
    
    
    df2.limit(1000).write.mode(SaveMode.Append).jdbc(outDriverUrl, outTableName+"copy", props)
  }
}