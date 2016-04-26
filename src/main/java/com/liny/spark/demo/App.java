package com.liny.spark.demo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		String str = "1983-07-27 dgd"; // 指定好一个日期格式的字符串
		String pat = "\\d{4}-\\d{2}-\\d{2}"; // 指定好正则表达式
		Pattern p = Pattern.compile(pat); // 实例化Pattern类
		Matcher m = p.matcher(str); // 实例化Matcher类
		
		if(m.find()){
			System.out.println(m.group());
		}
	}
}
