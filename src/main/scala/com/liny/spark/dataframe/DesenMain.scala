package com.liny.spark.dataframe

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import util.OracleHelper
import util.MysqlHelper
import java.sql.SQLException
import org.apache.spark.Logging
import java.sql.SQLSyntaxErrorException
import org.apache.spark.sql.SaveMode

object DesenMain extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Desensitize")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    Class.forName("oracle.jdbc.OracleDriver").newInstance();
    //    val df = sqlc.read
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true") // Use first line of all files as header
    //      .option("inferSchema", "false")
    //      .load("F:\\公司文档\\精诚瑞宝\\广发项目交接资料\\01_广发\\01_data\\sample-data.csv")

    val url: String = "jdbc:oracle:thin:system/systex@10.201.26.182:1521:sda"
    val outurl: String = "jdbc:oracle:thin:stp/systex@10.201.26.182:1521:sda"
    val driver: String = "oracle.jdbc.OracleDriver"
    val props = new java.util.Properties()
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

    val outTableName = "CARD_TRADE_RELA"
    val outTableName2 = "GUANGFA"

    val jdbcDF2 = sqlc.read.options(Map("url" -> url,
      "dbtable" -> outTableName))
      .format("jdbc").load().limit(1000)

    OracleHelper.init(url, props)
    //    val schema = OracleHelper.schemaString(jdbcDF2)
    //    println(schema)
    val metaInfo = OracleHelper.getTableMetaInfo("", outTableName)
    val createOracleTbSql = OracleHelper.createOracleTbSql(outTableName, metaInfo._2, metaInfo._1, metaInfo._3)
    println(createOracleTbSql)
    val indexList = metaInfo._4
    indexList.foreach(println)

    OracleHelper.init(outurl, props)
    val exists = OracleHelper.tableExists(outTableName)
    if (exists) {
      println("存在~")
    } else {
      println("不存在")
      try {
        OracleHelper.execQuery(createOracleTbSql)
      } catch {
        case e: SQLSyntaxErrorException => {
          log.error(s"创建表失败，SQL: $createOracleTbSql")
          e.printStackTrace()
          sys.exit(-1)
        }
        case ex: Exception => {
          log.error(s"创建表失败，SQL: $createOracleTbSql")
          ex.printStackTrace()
          sys.exit(-1)
        }
      }
      log.info(s"创建表${outTableName}成功!")
    }

    val nullTypes: Array[Int] = jdbcDF2.schema.fields.map { field =>
      OracleHelper.getJdbcType(field.dataType).jdbcNullType
    }
    OracleHelper.saveTable(jdbcDF2.rdd, nullTypes, outurl, jdbcDF2.schema, outTableName, props)

    //    OracleHelper.execQuery(createOracleTbSql)
    //    
    //    val mysqlprops = new java.util.Properties()
    //    mysqlprops.put("user", "root")
    //    mysqlprops.put("password", "root")
    //    MysqlHelper.init("jdbc:mysql://10.201.26.111:3306/stp_ding?zeroDateTimeBehavior=convertToNull&amp;characterEncoding=utf-8", mysqlprops)
    //    //如果存在这张表则删除
    //    if (MysqlHelper.tableExists(outTableName)) {
    //      MysqlHelper.dropTable(outTableName)
    //    }
    //    println(s"创建mysql数据表${outTableName}，SQL:${createMysqlTbSql}")
    //    MysqlHelper.execQuery(createMysqlTbSql)
    //    
    //     val primaryKeys = metaInfo._1
    //    
    //    jdbcDF2.printSchema()
    //    println("=======主键信息：" + primaryKeys.mkString(","))
    //    val primaryKeysIndex = new ArrayBuffer[Int]()
    //    val fields = jdbcDF2.schema.fields.map { f => f.name }
    //    println("ddd:"+fields.mkString(","))
    //    if (primaryKeys != null && primaryKeys.size > 0) {
    //      primaryKeys.foreach { key =>
    //        {
    //          if (fields.contains(key))
    //            primaryKeysIndex.+=(fields.indexOf(key))
    //        }
    //      }
    //    }
    //    println("=======主键信息2：" + primaryKeysIndex.mkString(","))
    //    //为了获取刚刚创建的mysql table的schema
    //    val mysqlDf = sqlc.read.options(Map("url" -> "jdbc:mysql://10.201.26.111:3306/stp_ding?zeroDateTimeBehavior=convertToNull&amp;characterEncoding=utf-8",
    //      "user" -> "root",
    //      "password" -> "root",
    //      "dbtable" -> outTableName))
    //      .format("jdbc").load()
    //    val mysqlSchema = mysqlDf.schema
    //    mysqlDf.printSchema()

    //    OracleHelper.init(url, props)
    //    val df2 = sqlc.createDataFrame(dat, jdbcDF2.schema)
    //    OracleHelper.saveTable(df2, url, outTableName, props)

    //    val df2 = sqlc.createDataFrame(dat, jdbcDF2.schema)
    //    OracleHelper.init(url, props)
    //    val columns = OracleHelper.getTableMetaInfo(outTableName2)
    //    columns._2.foreach(f => {println(f.toString())})
    //    println("------------------------")
    //    columns._1.foreach(f => {println(f.mkString(","))})
    //    val sql = OracleHelper.createMysqlTbSql(outTableName2,columns._2,columns._1)
    //    println(sql);
    //    val sche2 = OracleHelper.schemaString(jdbcDF2)
    //    println(s"$sche2")
    //    OracleHelper.insertStatement(outTableName, jdbcDF2.schema)
    //    OracleHelper.saveTable(df2, url, outTableName, props)

    //    val outDriverUrl = "jdbc:mysql://10.201.26.111:3306/stp_ding?user=root&password=root&zeroDateTimeBehavior=convertToNull&amp;characterEncoding=utf-8"
    //    val outDriverUrl = "jdbc:oracle:thin:system/systex@10.201.26.182:1521:sda"
    //    val outTableName = "testliny"
    //
    //    val url: String = "jdbc:oracle:thin:@10.201.26.182:1521/sda"
    //    val driver: String = "oracle.jdbc.OracleDriver"
    //    val props = new java.util.Properties()
    //    props.setProperty("user", "system")
    //    props.setProperty("password", "systex")
    //    props.setProperty("processEscapes", "false" );
    //    JdbcDialects.registerDialect(new OracleDialect)
    //    val jdbcDF2 = sqlc.read.options(Map("url" -> url,
    //      "user" -> "system",
    //      "password" -> "systex",
    //      "dbtable" -> "\"credit_card_info\""))
    //      .format("jdbc").load()
    //    val schema = StructType(jdbcDF2.schema.fields.map(fieldName => StructField(fieldName.name.toUpperCase(), StringType, true)))
    //    println(schema.treeString)

    //    OracleHelper.init(url, props)
    //    OracleHelper.insertStatement(outTableName, jdbcDF2.schema)
    //    val df2 = sqlc.createDataFrame(jdbcDF2.rdd, jdbcDF2.schema)
    //    OracleHelper.saveTable(df2, url, outTableName, props)
    //    val df2 = sqlc.createDataFrame(jdbcDF2.rdd,jdbcDF2.schema)

    //    JdbcUtils.createConnection(url, props).createStatement().e
    //    JdbcUtils.saveTable(df2, url, outTableName.toUpperCase(), props)
    //    df2.createJDBCTable(url, outTableName, true)
    //    jdbcDF2.write.mode(SaveMode.Overwrite).jdbc(url,outTableName.toUpperCase(), props)

  }
}