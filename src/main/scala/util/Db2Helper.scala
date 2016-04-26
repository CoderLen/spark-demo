package util

import org.apache.spark.Logging
import java.sql.DriverManager
import org.apache.spark.sql.types.StructType
import breeze.linalg.sum
import org.apache.spark.sql.Row
import java.sql.Connection
import java.sql.Statement
import scala.util.Try
import java.sql.SQLException
import java.util.Properties
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType

/**
 * DB2 工具类
 */
object Db2Helper extends DBHelpler with Serializable with Logging {

  private var url: String = null
  private var connpro: Properties = null

  def init(_url: String, _connpro: Properties) {
    this.url = _url
    this.connpro = _connpro
  }

  override def getConnection(): Connection = {
    log.info("class:com.ibm.db2.jcc.DB2Driver")
    log.info("url:" + url)
    Class.forName("com.ibm.db2.jcc.DB2Driver")
    val connection = DriverManager.getConnection(url)
    log.info("连接信息：" + connection.toString())
    connection
  }
  
  override def getJdbcType(dt: DataType): JdbcType = {
    getDb2Type(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }
  
  private def getDb2Type(dt: DataType): Option[JdbcType] = dt match {
     case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case BooleanType => Option(JdbcType("CHAR(1)", java.sql.Types.CHAR))
    case _ => None
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
          for (j <- 0 until fieldSize) {
            params.append(s"'${row(j)}',")
          }
          queryBuffer.append(params.substring(0, params.length - 1)).append(")")
          psmt.addBatch(queryBuffer.toString)
        }
      }
      psmt.executeBatch()
      num += sum(psmt.executeBatch())
      connection.commit()
      log.info(s"提交 $num 条数据.........")
    } catch {
      case sqlEx: SQLException => {
        var next = sqlEx.getNextException()
        while(next != null){
          next.printStackTrace()
          next = next.getNextException
        }
        sqlEx.printStackTrace()
      }
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (psmt != null) {
        psmt.close()
      }
      closeConnection(connection)
    }
  }

}