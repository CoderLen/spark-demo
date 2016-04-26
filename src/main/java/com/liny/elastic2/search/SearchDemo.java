package com.liny.elastic2.search;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.liny.elastic2.util.ElasticAdminUtil;

public class SearchDemo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Map<String,Object> settingMap = new HashMap<String,Object>();
		String index = "zhihu-crawler";
		ElasticAdminUtil.init();
		ElasticAdminUtil.createIndex(index);
	}
}
