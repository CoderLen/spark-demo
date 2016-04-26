package util

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.Properties
import scala.util.Try
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import breeze.linalg.sum
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.DataType

/**
 * MySQL 工具类
 */
object MysqlHelper extends DBHelpler with Serializable with Logging {

  private var url: String = null
  private var connpro: Properties = null

  def init(_url: String, _connpro: Properties) {
    this.url = _url
    this.connpro = _connpro
  }

  override def getConnection(): Connection = {
    val conn = DriverManager.getConnection(url, connpro.getProperty("user"), connpro.getProperty("password"))
    conn
  }
  
  override def getJdbcType(dt: DataType): JdbcType = {
    getCommonJDBCType(dt).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
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
    val psmtSql = insertStatementSql(table, rddSchema)
    val isExists = tableExists(table)
    if (!isExists) {
      throw new IllegalArgumentException(s"the table $table doesn't exist!")
    }
    try {
      connection.setAutoCommit(false)
      psmt = connection.createStatement()
      val fieldSize = rddSchema.fields.size;
      log.info(s"rddSchema fields size: ${rddSchema.fields.size}")
            ///循环导入
      iterator.foreach { row =>
        {
//          log.info(s"row length: ${row.length}")
//          log.info("mysql row string:"+row)
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
            params.append(s"'${row(j)}',")
          }
          queryBuffer.append(params.substring(0, params.length - 1)).append(")")
//          println("sql string:"+queryBuffer.toString)
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
        connection.close()
      }
    }
  }

}