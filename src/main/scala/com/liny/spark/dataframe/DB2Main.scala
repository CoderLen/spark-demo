package com.liny.spark.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import util.JdbcDialects

object DB2Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Desensitize")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    Class.forName("com.ibm.db2.jcc.DB2Driver");
    //    val df = sqlc.read
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true") // Use first line of all files as header
    //      .option("inferSchema", "false")
    //      .load("F:\\公司文档\\精诚瑞宝\\广发项目交接资料\\01_广发\\01_data\\sample-data.csv")

    val url: String = "jdbc:db2://10.201.26.41:50000/test?user=db2inst&password=systex"
    val driveUrl = "jdbc:db2://10.201.26.41:50000/test:user=db2inst;password=systex;useUnicode=true;";
   val jdbcInfo = JdbcDialects.getDbInfo(url)

    val outTableName = "mytable"
    val outTableName2 = "GUANGFA"

    val jdbcDF2 = sqlc.read.options(Map("url" -> driveUrl,
      "dbtable" -> outTableName))
      .format("jdbc").load()
      
    jdbcDF2.printSchema()

//    Db2Helper.init(url, jdbcInfo._2)
//    val metaInfo = Db2Helper.getTableMetaInfo("",outTableName)
//    val createMysqlTbSql = Db2Helper.createMysqlTbSql(outTableName, metaInfo._2, metaInfo._1)
//    
//    println("createMysqlTbSql:"+createMysqlTbSql)
   


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