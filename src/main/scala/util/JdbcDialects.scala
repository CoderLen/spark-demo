package util


import java.util.Properties

/**
 * 
 */
object JdbcDialects {

  private val reg = "(\\?|\\&|\\;|\\:)\\S+(\\?|\\&|\\Z|\\;)".r

  /**
   * 获取数据库信息
   */
  def getDbInfo(url: String): (String, Properties) = {
    val dbType = getDbType(url)
    val prop = new Properties
    dbType match {
      case "mysql" => {
        val propsStr = reg.findFirstIn(url).get
        val propArray = propsStr.substring(1, propsStr.lastIndexOf("&")).split("&")
        val propTulpe = propArray.map { prop => { val temp = prop.split("="); (temp(0), temp(1)) } }
        propTulpe.foreach { p =>
          {
            prop.put(p._1, p._2)
          }
        }
      }
      case "oracle" => {
        val temp = url.split(":").apply(3)
        if (temp.contains("/")) {
          val userAndpwd = temp.substring(0, temp.indexOf("@")).split("/")
          prop.put("user", userAndpwd(0))
          prop.put("password", userAndpwd(1))
        }
      }
      case "db2" => {
        val temp = url.substring(url.lastIndexOf(":"),url.length())
        val propsStr = reg.findFirstIn(temp).get
        val dburl = url.substring(0,url.lastIndexOf(":"))
        prop.put("url", dburl)
        val propArray = propsStr.substring(1).split(";")
        val propTulpe = propArray.map { prop => { val temp = prop.split("="); (temp(0), temp(1)) } }
        propTulpe.foreach { p =>
          {
            prop.put(p._1, p._2)
          }
        }
      }
      case _ => {
        throw new IllegalArgumentException(s"Can't get JDBC type for ${url}")
      }
    }
    (dbType, prop)
  }

  /**
   * 获取数据库类型
   */
  def getDbType(url: String): String = {
    if (url.startsWith("jdbc:mysql")) {
      "mysql"
    } else if (url.startsWith("jdbc:oracle")) {
      "oracle"
    } else if (url.startsWith("jdbc:db2")) {
      "db2"
    } else {
      throw new IllegalArgumentException(s"Can't get JDBC type for ${url}")
    }
  }
}