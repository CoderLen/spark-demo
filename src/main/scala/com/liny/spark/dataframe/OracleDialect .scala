package com.liny.spark.dataframe

/**
 * Created by st84879 on 26/01/2016.
 */

import java.sql.{ Connection, Types }
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.AtomicType

protected class OracleDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")

  override def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    // Handle NUMBER fields that have no precision/scale in special way
    // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
    // For more details, please see
    // https://github.com/apache/spark/pull/8780#issuecomment-145598968
    // and
    // https://github.com/apache/spark/pull/8780#issuecomment-144541760
    if (sqlType == Types.NUMERIC && size == 0) {
      // This is sub-optimal as we have to pick a precision/scale in advance whereas the data
      //  in Oracle is allowed to have different precision/scale for each value.
      Option(DecimalType(DecimalType.MAX_PRECISION, 10))
    } else {
      None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
    case t: DecimalType => Option(
      JdbcType(s"DECIMAL(${t.precision},${changeScale(t.scale)})", java.sql.Types.DECIMAL))
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
} 