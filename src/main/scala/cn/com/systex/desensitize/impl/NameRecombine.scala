package cn.com.systex.desensitize.impl

import com.liny.util.GenerateDataUtil

import cn.com.systex.desensitize.Algorithm
/**
 * 姓名重组算法--4
 */
class NameRecombine extends Algorithm with Serializable {

  override def run(content: String): String = {
    GenerateDataUtil.generateName()
  }
}