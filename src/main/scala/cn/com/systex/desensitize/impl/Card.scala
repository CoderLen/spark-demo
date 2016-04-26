package cn.com.systex.desensitize.impl

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Random
import cn.com.systex.desensitize.Algorithm
import com.liny.util.GenerateDataUtil

/**
 * 身份证脱敏规则
 */
class Card extends Algorithm with Serializable{

  private val rand = new Random()
  private val dateFormat = new SimpleDateFormat("yyyyMMdd")

  override def run(idCard: String): String = {
    var result: String = idCard
    if (idCard.length == 18) {
      val birth = idCard.substring(6, 14)
      val newDate = changeDate(birth)
      result = idCard.replace(birth, newDate)
      val lastFour = idCard.substring(14)
      val newLastFour = GenerateDataUtil.randomNumberStr(4)
      result = result.replace(lastFour, newLastFour)
    } else if (idCard.length == 15) {
      val birth = idCard.substring(6, 12)
      val newDate = changeDate(s"19${idCard.substring(6, 12)}")
      result = idCard.replace(birth, newDate.substring(2))
      val lastThree = idCard.substring(9)
      val newLastThree = GenerateDataUtil.randomNumberStr(3)
      result = result.replace(lastThree, newLastThree)
    }
    result
  }
  
  /**
   * 随机改变日期
   */
  def changeDate(dateStr: String): String = {
    val date = dateFormat.parse(dateStr)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.YEAR, rand.nextInt(5));
    calendar.add(Calendar.MONTH, rand.nextInt(12));
    calendar.add(Calendar.DAY_OF_MONTH, rand.nextInt(31));
    dateFormat.format(calendar.getTime)
  }

}
