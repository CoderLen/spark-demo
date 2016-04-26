package util

import java.sql.Connection
import java.sql.PreparedStatement
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

abstract class DBHelpler extends Logging {

  def insertData(table: String,
    iterator: Iterator[Row],
    rddSchema: StructType,
    batchSize: Int)

  /**
   * 执行SQL
   */
  def execQuery(sql: String): Unit = {
    val connection: Connection = getConnection()
    val stat = connection.createStatement()
    try {
      stat.executeUpdate(sql)
    } finally {
      if (stat != null) {
        stat.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
  }

  def getDbType(url: String): String = {
    if (url.startsWith("jdbc:mysql")) {
      "mysql"
    } else if (url.startsWith("jdbc:oracle")) {
      "oracle"
    } else if (url.startsWith("jdbc:db2")) {
      "db2"
    }
    ""
  }

  /**
   * 获取连接
   */
  def getConnection(): Connection = {
    null
  }

  /**
   * 获取连接
   */
  def getConnection(url: String): Connection = {
    null
  }

  /**
   * 生成插入表数据的statment 部分 Sql
   */
  def insertStatementSql(table: String, rddSchema: StructType): String = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES "
    log.info(s"SQL语句：$sql")
    sql
  }

  /**
   * 返回插入表数据的PreparedStatement
   */
  def insertStatement(table: String, rddSchema: StructType): String = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    log.info(s"SQL语句：$sql")
    sql
  }

  /**
   * 获取表信息<br>
   * 主键信息和列信息(字段名，字段类型，字段长度，字段精度，是否允许为空)
   */
  def getTableMetaInfo(userName: String, tableName: String): (List[String], List[(String, String, Int, Int, Int)], List[(String, String, String)],List[String]) = {
    var schema = userName
    if (schema.trim().equals("")) {
      schema = null
    } else {
      schema = schema.toUpperCase()
    }

    val connection = getConnection()
    val catalog = connection.getCatalog() //catalog 其实也就是数据库名  
    val metaData = connection.getMetaData()

    //获取主键信息
    val primaryKeyResultSet = metaData.getPrimaryKeys(catalog, schema, tableName)
    val primaryKeys = new ArrayBuffer[String]()
    while (primaryKeyResultSet.next()) {
      val primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME")
      if (!primaryKeys.contains(primaryKeyColumnName)) {
        primaryKeys.+=(primaryKeyColumnName)
      }
    }

    //获取外键信息
    val foreignKeyResultSet = metaData.getImportedKeys(catalog, schema, tableName)
    val foreignKeys = new ArrayBuffer[(String, String, String)]()
    while (foreignKeyResultSet.next()) {
      val fkColumnName = foreignKeyResultSet.getString("FKCOLUMN_NAM");
      val pkTablenName = foreignKeyResultSet.getString("PKTABLE_NAME");
      val pkColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME");
      if (!foreignKeys.contains(fkColumnName)) {
        foreignKeys.+=((fkColumnName, pkTablenName, pkColumnName))
      }
    }

    //获取索引信息
    val indexInformation = metaData.getIndexInfo(connection.getCatalog, null, tableName, false, true)
    val createIndexList = new ArrayBuffer[String]
    import scala.collection.mutable.Map
    import scala.collection.mutable.ArrayBuffer
    val indexsMap = Map[(String, String, String, String), ArrayBuffer[String]]()
    while (indexInformation.next()) {
      val dbSchema = indexInformation.getString("TABLE_SCHEM");
      val dbTableName = indexInformation.getString("TABLE_NAME");
      val dbNoneUnique = indexInformation.getBoolean("NON_UNIQUE");
      val dbIndexQualifier = indexInformation.getString("INDEX_QUALIFIER");
      val dbIndexName = indexInformation.getString("INDEX_NAME");
      val dbType = indexInformation.getShort("TYPE");
      val dbOrdinalPosition = indexInformation.getShort("ORDINAL_POSITION");
      val dbColumnName = indexInformation.getString("COLUMN_NAME");
      val dbAscOrDesc = indexInformation.getString("ASC_OR_DESC");
      val dbCardinality = indexInformation.getInt("CARDINALITY");
      val dbPages = indexInformation.getInt("PAGES");
      val dbFilterCondition = indexInformation.getString("FILTER_CONDITION");

      var unique = "";
      if (!dbNoneUnique) {
        unique = "UNIQUE "
      }
      if (dbIndexName != null) {
        if (indexsMap.contains((dbIndexName, unique, dbSchema, dbTableName))) {
          val indexDetail = indexsMap.get((dbIndexName, unique, dbSchema, dbTableName)).get
          indexDetail.+=:(dbColumnName)
          indexsMap((dbIndexName, unique, dbSchema, dbTableName)) = indexDetail
        } else {
          val indexDetail = ArrayBuffer.empty[String]
          indexDetail.+=:(dbColumnName)
          indexsMap((dbIndexName, unique, dbSchema, dbTableName)) = indexDetail
        }
      }
    }
    indexsMap.foreach(f => {
      val sql = new StringBuffer()
      val indexInfo = f._1
      val indexColumn = f._2
      sql.append(s"CREATE ${indexInfo._2}INDEX ${indexInfo._1} ${indexInfo._4} (${indexColumn.mkString(",")})")
      createIndexList.+=:(sql.toString)
    })

    //获取列信息
    val colRet = metaData.getColumns(catalog, schema, tableName, null)
    val columnInfos = ArrayBuffer.empty[(String, String, Int, Int, Int)]
    while (colRet.next) {
      val columnName = colRet.getString("COLUMN_NAME")
      val columnType = colRet.getString("TYPE_NAME")
      val datasize = colRet.getInt("COLUMN_SIZE")
      val digits = colRet.getInt("DECIMAL_DIGITS")
      val nullable = colRet.getInt("NULLABLE")
      val tulpe = (columnName, columnType, datasize, digits, nullable)
      val cols = columnInfos.map(f => f._1)
      if (!cols.contains(columnName))
        columnInfos.+=(tulpe)
    }
    connection.close()
    (primaryKeys.toList, columnInfos.toList, foreignKeys.toList,createIndexList.toList)
  }

  /**
   * 生成Mysql表的数据库sql
   */
  def createMysqlTbSql(tableName: String, columns: List[(String, String, Int, Int, Int)], primaryKeys: List[String], columnNames: Seq[String]): String = {
    log.info(s"生成表${tableName}的sql..")
    log.info(s"主键:${primaryKeys.mkString(",")}")
    log.info(s"列信息:${columns.mkString(",")}")
    log.info(s"规则列信息:${columnNames.mkString(",")}")
    val sql = new StringBuffer(s"CREATE TABLE $tableName(")
    val columnSql = new StringBuffer()

    if (columnNames == null && columnNames.size < 1) {
      log.error(s"no rules about this about this table $tableName!")
      sys.exit(-1)
    }

    columns.foreach(column => {
      if (columnNames.contains(column._1)) {
        columnSql.append(s"${column._1} varchar(${column._3 * 2 + 20}) ,")
      }
    })

    if (primaryKeys != null && primaryKeys.size > 0) {
      var primarySize = 0
      columns.foreach(column => {
        if (primaryKeys.contains(column._1)) {
          primarySize = primarySize + column._3
        }
      })
      if (primarySize == 0) {
        primarySize = 150
      }
      sql.append(s"TUOMINID varchar(${primarySize + 20}) not null primary key,")
    }
    sql.append(columnSql.substring(0, columnSql.length - 1)).append(")").toString()
  }

  /**
   * Drops a table from the JDBC database.
   */
  def dropTable(table: String): Unit = {
    log.info(s"Drop Table $table !")
    val connection = getConnection
    val statement = connection.createStatement
    try {
      statement.executeUpdate(s"DROP TABLE $table")
    } finally {
      statement.close()
      connection.close()
    }
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(tableName: String): Boolean = {
    Try {
      log.info("判断表是否存在~")
      val connection = getConnection
      val statement = connection.prepareStatement(getTableExistsQuery(tableName))
      try {
        statement.executeQuery()
      } finally {
        statement.close()
        connection.close()
      }
    }.isSuccess
  }

  /**
   * Get the SQL query that should be used to find if the given table exists. Dialects can
   * override this method to return a query that works best in a particular database.
   * @param table  The name of the table.
   * @return The SQL query to use for checking the table.
   */
  def getTableExistsQuery(tableName: String): String = {
    s"SELECT * FROM $tableName WHERE 1=0"
  }

  /**
   * Compute the schema string for this RDD.
   */
  def schemaString(df: DataFrame): String = {
    val sb = new StringBuilder()
    df.schema.fields foreach { field =>
      val name = field.name
      val typ: String = getJdbcType(field.dataType).databaseTypeDefinition
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  def getJdbcType(dt: DataType): JdbcType

  /**
   * Retrieve standard jdbc types.
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The default JdbcType for this DataType
   */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
    rowRDD: RDD[Row],
    url: String,
    schema: StructType,
    table: String,
    connpro: Properties) {

    val batchSize = 500
    rowRDD.foreachPartition { iterator =>
      insertData(table, iterator, schema, batchSize)
    }
  }

  /**
   * 关闭连接
   */
  def closeConnection(connection: Connection) {
    try {
      if (connection != null) {
        if (connection.isReadOnly()) {
          log.info("connection is readonly, therefore rollback")
          connection.rollback()
        } else {
          log.info("connection is not readonly, therefore commit")
          connection.commit()
        }
        connection.setAutoCommit(true)
        connection.close();
      }
    } catch {
      case e: Exception => { log.error("Ignoring Error when closing connection", e) };
    }
  }
}