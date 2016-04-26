package util

import java.sql.Connection
import java.sql.Statement
import java.util.Properties
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import breeze.linalg.sum
import oracle.jdbc.driver.OracleDriver
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.ArrayType
import scala.util.control.NonFatal
import org.apache.spark.rdd.RDD

/**
 * Created by liny on 20160414.
 * Oracle 工具类
 */
object OracleHelper extends DBHelpler with Serializable with Logging {

  private var url: String = null
  private var connpro: Properties = null

  def init(_url: String, _connpro: Properties) {
    this.url = _url
    this.connpro = _connpro
  }
  
  /**
   * 获取连接
   */
  override def getConnection(): Connection = {
    val conn = {
      val oracle = new OracleDriver()
      try {
        Some(oracle.connect(url, connpro)).get
      } catch {
        case e: Exception =>
          sys.exit(1)
      }
    }
    conn
  }
  
  override def getJdbcType(dt: DataType): JdbcType = {
    getOracleType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }
  
  private def getOracleType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
    case _ => None
  }


  private def changeScale(scale: Int): Int = {
    if (scale <= -84) {
      -83
    } else if (scale >= 127) {
      127
    } else {
      scale
    }
  }
  
  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction in order to avoid repeatedly inserting
   * data as much as possible.
   *
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
   *
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
   */
  def savePartition(
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      nullTypes: Array[Int],
      batchSize: Int): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
      conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        logWarning("Exception while detecting transaction support", e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val stmtSql = insertStatement(table, rddSchema)
      val stmt = conn.prepareStatement(stmtSql)
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case ArrayType(et, _) =>
                  // remove type length parameters from end of type name
                  val typeName = getJdbcType(et).databaseTypeDefinition
                    .toLowerCase.split("\\(")(0)
                  val array = conn.createArrayOf(
                    typeName,
                    row.getSeq[AnyRef](i).toArray)
                  stmt.setArray(i + 1, array)
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }

 
  /**
   * Saves the RDD to the database in a single transaction.
   */
   def saveTable(
    rowRDD: RDD[Row],
    nullTypes: Array[Int],
    url: String,
    schema: StructType,
    table: String,
    connpro: Properties) {

    val batchSize = 500
    rowRDD.foreachPartition { iterator =>
      savePartition(table, iterator, schema, nullTypes,batchSize)
    }
  }

  
    /**
   * 生成Mysql表的数据库sql
   */
  def createOracleTbSql(tableName: String, columns: List[(String, String, Int, Int, Int)], primaryKeys: List[String],foreignKeys:List[(String,String,String)]): String = {
    log.info(s"生成表${tableName}的sql..")
    log.info(s"主键:${primaryKeys.mkString(",")}")
    log.info(s"列信息:${columns.mkString(",")}")
    val sql = new StringBuffer(s"CREATE TABLE $tableName(")

    columns.foreach(column => {
        if(column._2.equals("NUMBER")){
          sql.append(s"${column._1} ${column._2}(${column._3},${changeScale(column._4)}) ,")
        }else if(column._2.contains("TIMESTAMP")){
          sql.append(s"${column._1} ${column._2} ,")
        }else{
          sql.append(s"${column._1} ${column._2}(${column._3 * 2 + 20}) ,")
        }
    })

    if (primaryKeys != null && primaryKeys.size > 0) {
      sql.append(s"CONSTRAINT PK_${primaryKeys.mkString("_")} PRIMARY KEY (${primaryKeys.mkString(",")}),")
    }
    if (foreignKeys != null && foreignKeys.size > 0 ){
      foreignKeys.foreach( f => {
         sql.append(s"CONSTRAINT FK_${foreignKeys.mkString("_")} FOREIGN KEY (${f._1}) REFERENCES ${f._2}(${f._3}),")
      })
    }
    sql.deleteCharAt(sql.lastIndexOf(","))
    
    .append(")").toString()
  }

  /**
   * 插入数据
   */
  def insertData(table: String,
    iterator: Iterator[Row],
    rddSchema: StructType,
    batchSize: Int): Unit = {
    var psmt: Statement = null
    var i = 0
    var num = 0
    val connection = getConnection()
    val fieldSize = rddSchema.fields.size;
    val psmtSql = insertStatementSql(table, rddSchema)
    try {
      connection.setAutoCommit(false)
      psmt = connection.createStatement()
      psmt.execute("alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss'");
      iterator.foreach { row =>
        {
//          log.info(s"row length: ${row.length}")
//          println("row string:"+row)
          var queryBuffer = new StringBuffer(psmtSql)
          i += 1
          if (i > batchSize) {
            i = 0
            psmt.executeBatch()
            num += sum(psmt.executeBatch())
            connection.commit()
            psmt.clearBatch()
          }
          val params = new StringBuffer("(");
          for (j <- 0 until row.length) {
            params.append("'" + s"${row(j)}" + "'" + ",")
          }
          queryBuffer.append(params.substring(0, params.length - 1)).append(")")
//          println("oracle sql string:"+queryBuffer.toString)
          psmt.addBatch(queryBuffer.toString)
        }
      }
      psmt.executeBatch()
      num += sum(psmt.executeBatch())
      connection.commit()
      log.info(s"提交 $num 条数据.........")
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (psmt != null) {
        psmt.close()
      }
      if (connection != null) {
        connection.setAutoCommit(true)
        connection.close()
      }
    }
  }
}
