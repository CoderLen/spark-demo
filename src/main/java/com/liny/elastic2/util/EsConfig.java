package com.liny.elastic2.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * ElasticSearch 配置类
 * 
 * @author 泳
 * @date 2015年11月1日
 */
public class EsConfig {

	public static EsConfig config = null;

	private static final String CLUSTERNAME = "clusterName";

	private static final String HOSTS = "hosts";

	private static final String PORT = "port";

	/**
	 * ElasticSearch 集群名称
	 */
	private String clusterName;

	/**
	 * ElasticSearch TCP服务端口号
	 */
	private String port;

	/**
	 * ElasticSearch 集群主机IP列表，一个或多个
	 */
	private String[] hosts;

	private EsConfig() {
	}

	/**
	 * 读配置文件es-config.properties
	 * 
	 * @return
	 */
	private Properties readConfig() {
		Properties props = new Properties();
		try {
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream("es-config.properties");
			props.load(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return props;
	}

	/**
	 * 根据prop配置，初始化config对象
	 */
	private void init() {
		Properties pros = this.readConfig();
		String clustername = pros.getProperty(EsConfig.CLUSTERNAME);
		if (clustername != null) {
			config.clusterName = clustername;
		}
		String hosts = pros.getProperty(EsConfig.HOSTS);
		if (hosts != null) {
			String addresss = hosts.toString();
			if (addresss.indexOf(",") != -1) {
				config.hosts = addresss.split(",");
			} else {
				config.hosts = new String[] { addresss };
			}
		}
		String port = pros.getProperty(EsConfig.PORT);
		if (port != null) {
			config.port = port;
		}
	}

	/**
	 * 返回ESConfig实例
	 * 
	 * @return
	 */
	public static EsConfig getIntance() {
		if (config == null) {
			config = new EsConfig();
			config.init();
		}
		return config;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String[] getHosts() {
		return hosts;
	}

	public void setHosts(String[] hosts) {
		this.hosts = hosts;
	}

	public static void main(String[] args) {
		EsConfig esConfig = EsConfig.getIntance();
		System.out.println(esConfig.toString());
	}

}
