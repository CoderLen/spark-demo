package cn.com.systex.desensitize.impl

import com.liny.util.GenerateDataUtil

import cn.com.systex.desensitize.Algorithm
/**
 * 地址重组算法--5
 */
class AddrRecombine extends Algorithm with Serializable{

  override def run(content: String): String = {
    val city = GenerateDataUtil.getRandomCity()
    GenerateDataUtil.getRandomAddr(city)
  }
}
