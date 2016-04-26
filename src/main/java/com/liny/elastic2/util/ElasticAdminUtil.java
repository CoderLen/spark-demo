package com.liny.elastic2.util;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * ElasticSearch 索引管理工具类
 * @author 泳
 * @date 2015年11月27日
 */
public class ElasticAdminUtil {

	private static Client client = null;

	private static AdminClient adminClient = null;

	private static Logger log = LoggerFactory.getLogger(ElasticAdminUtil.class);

	/**
	 * 初始化client
	 */
	public static void init() {
		if (adminClient == null) {
			adminClient = getAdminClient();
		}
	}

	/**
	 * 初始化client
	 * 
	 * @param esClient
	 *            实例化的client对象
	 */
	public static void init(Client esClient) {
		if (client == null) {
			client = esClient;
			adminClient = client.admin();
		}
	}

	/**
	 * 获取ElasticSearch连接客户端
	 * 
	 * @return
	 */
	public static AdminClient getAdminClient() {
		/* 获取ES配置 */
		EsConfig config = EsConfig.getIntance();
		EsClient esClient = new EsClient(config.getClusterName(), config.getHosts(),
				Integer.parseInt(config.getPort()));
		client = esClient.transportClient();
		return client.admin();
	}

	/**
	 * 创建新索引
	 * 
	 * @param index
	 *            索引名称
	 * @throws IOException
	 */
	public static void createIndex(String index) throws IOException {
		adminClient.indices().prepareCreate(index).execute().actionGet();
	}

	/**
	 * 创建新索引，并指定索引的settings
	 * 
	 * @param index
	 * @param settingMap
	 */
	public static void createIndex(String index, Map<String, Object> settingMap) {
		Builder builder = Settings.settingsBuilder();
		for (Entry<String, Object> entry : settingMap.entrySet()) {
			builder.put(entry.getKey(), entry.getValue());
		}
		Settings settings = builder.build();
		adminClient.indices().prepareCreate(index).setSettings(settings).execute().actionGet();
	}

	/**
	 * 删除整个索引
	 * 
	 * @param index
	 * @param type
	 */
	public static void deleteIndex(String index) {
		adminClient.indices().prepareDelete(index).execute().actionGet();
	}

	public static void changeDefaultMapping(String index, String type,Map<String, Map<String, Object>> mappingMap) throws IOException {
		if (!existsIndex(index)) {
			log.warn("索引" + index + "不存在,准备创建索引.");
			createIndex(index);
		} else {
			if (existsType(index,type)) {
				log.warn("索引" + index + "已存在type:" + type + ",索引将被更新..");
			}
		}
		XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true)
				.field("dynamic","strict").endObject();
		mapping.startObject("properties");
		for (Entry<String, Map<String, Object>> entry : mappingMap.entrySet()) {
			mapping.startObject(entry.getKey());
			for (Entry<String, Object> kv : entry.getValue().entrySet()) {
				mapping.field(kv.getKey(), kv.getValue());
			}
			mapping.endObject();
		}
		mapping.endObject();
		PutMappingRequest putMappingRequest = new PutMappingRequest(index);
		putMappingRequest.type(type);
		putMappingRequest.source(mapping);
		adminClient.indices().putMapping(putMappingRequest).actionGet();
	}

	/**
	 * 为一个索引创建mapping
	 * 
	 * @param index
	 *            索引名
	 * @param type
	 * @param mappingMap
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void createMapping(String index, String type, Map<String, Map<String, Object>> mappingMap)
			throws IOException {
		if (!existsIndex(index)) {
			log.warn("索引" + index + "不存在,准备创建索引.");
			createIndex(index);
		} else {
			if (existsType(index,type)) {
				log.warn("索引" + index + "已存在type:" + type + ",索引将被更新..");
			}
		}
		XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true)
				.endObject().startObject("properties");
		for (Entry<String, Map<String, Object>> entry : mappingMap.entrySet()) {
			mapping.startObject(entry.getKey());
			for (Entry<String, Object> kv : entry.getValue().entrySet()) {
				mapping.field(kv.getKey(), kv.getValue());
			}
			mapping.endObject();
		}
		mapping.endObject();
		PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type).source(mapping);
		adminClient.indices().putMapping(mappingRequest).actionGet();
	}

	/**
	 * 对一个index创建mapping
	 * 
	 * @param index
	 *            索引名
	 * @param type
	 *            类型
	 * @param jsonObject
	 *            封装了Mapping的Json对象
	 */
	public static void createMapping(String index, String type, JSONObject jsonObject) {
		PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type).source(jsonObject); // 封装putMapping请求
		adminClient.indices().putMapping(mappingRequest).actionGet();
	}

	/**
	 * 关闭索引
	 * 
	 * @param index
	 */
	public static void closeIndex(String index) {
		CloseIndexRequest closeIndexReq = new CloseIndexRequest(index);
		adminClient.indices().close(closeIndexReq);
	}

	/**
	 * 打开索引
	 * 
	 * @param index
	 */
	public static void openIndex(String index) {
		OpenIndexRequest openIndexReq = new OpenIndexRequest(index);
		adminClient.indices().open(openIndexReq);
	}

	/**
	 * 判断索引是否存在
	 * 
	 * @param index
	 * @return
	 */
	public static boolean existsIndex(String index) {
		IndicesExistsRequest rq = new IndicesExistsRequest().indices(new String[] { index });
		return adminClient.indices().exists(rq).actionGet().isExists();
	}

	/**
	 * 判断index里是否存在type
	 * @param index
	 * @param type
	 * @return
	 */
	public static boolean existsType(String index,String type){
		TypesExistsRequest typeRequest = new TypesExistsRequest(new String[] { index }, type);
		return adminClient.indices().typesExists(typeRequest).actionGet().isExists();
	}
	/**
	 * 关闭client连接
	 */
	public static void closeClient() {
		client.close();
	}
}
