package com.liny.spark.dataframe

import java.util.Properties
import scala.util.Properties
import scala.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import com.liny.util.GenerateDataUtil
import cn.com.systex.desensitize.RegularReplace
import cn.com.systex.desensitize.impl.Md5Encryp
import cn.com.systex.desensitize.impl.NameRecombine
import scala.collection.mutable.ArrayBuffer

object GuangfaMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Desensitize")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val driverUrl = "jdbc:mysql://10.201.26.111:3306/stp_ding?user=root&password=root&zeroDateTimeBehavior=convertToNull&amp;characterEncoding=utf-8"
    val tableName = "guangfa"
    
    
    val outDriverUrl = "jdbc:mysql://10.201.26.111:3306/stp_ding?user=root&password=root&zeroDateTimeBehavior=convertToNull&amp;characterEncoding=utf-8"
    val outTableName = "guangfa20160413"


    var al = new NameRecombine
    var md5 = new Md5Encryp()
    var reg = new RegularReplace("[a-zA-Z0-9_-]+?@", "*******")
    //从MYSQL读取数据
    val jdbcDF = sqlc.read
      .options(Map("url" -> driverUrl,
        //      "user" -> "root",
        //      "password" -> "root",
        "dbtable" -> tableName))
      .format("jdbc")
      .load()

    val fields = jdbcDF.schema.fields
    val newRow = jdbcDF.map { row =>
      {
        var seqs = row.toSeq.toBuffer
        fields.map { field =>
          {
            val fieldName = field.name
            val index = row.fieldIndex(field.name);
            seqs(index) = reg.run(row.getString(index))
          }
        }
        Row.fromSeq(seqs.toSeq)
      }
    }
    val df2 = sqlc.createDataFrame(newRow, jdbcDF.schema)
    df2.write.mode(SaveMode.Overwrite).jdbc(outDriverUrl, outTableName, new Properties)
    //    
    //    val df = sqlc.read
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true") // Use first line of all files as header
    //      .option("inferSchema", "false")
    //      .load("F:\\公司文档\\精诚瑞宝\\广发项目交接资料\\01_广发\\01_data\\sample-data.csv")
    //
    //    val data = sc.wholeTextFiles("G:\\test\\data\\*", 3)
    //    val lineDat = data.map {
    //      case (filename, content) => {
    //        val lines = content.split("\r\n|\n") //分割行
    //        lines.toVector
    //      }
    //    }
    //
    //    import sqlc.implicits._
    //    val dat = lineDat.flatMap(lines => {
    //      lines.map { line =>
    //        {
    //          val values = line.split(",").toList
    //          Row.fromSeq(values)
    //        }
    //      }
    //    })
    //
    //    val df2 = sqlc.createDataFrame(newDf, df.schema)
    //    //          df1.write.jdbc(driverUrl, "sparktomysql", new Properties)
    //    df2.write.mode(SaveMode.Overwrite).jdbc(driverUrl, "guangfa", new Properties)

  }
}