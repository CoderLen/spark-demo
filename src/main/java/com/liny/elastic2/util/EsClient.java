package com.liny.elastic2.util;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.net.InetSocketAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

public class EsClient {

	/**
	 * 集群名称
	 */
	private String clusterName;
	/**
	 * ES集群的ip，填一个或两个节点IP
	 */
	private String[] addressArray;
	/**
	 * ES TCP端口号
	 */
	private int port;

	public EsClient(String clusterName, String[] addressArray, int port) {
		this.clusterName = clusterName;
		this.addressArray = addressArray;
		this.port = port;
	}

	/**
	 * NodeClient的构建方式 NodeBuilder采用建造者的方式构建Node，可以指定参数， clustrName：集群名字
	 * Client：Node的角色限定为client，不存储数据 local：本地模式，适用于测试开发
	 * 
	 * @return
	 */
	public Client nodeClient() {
		/**
		 * 两种方式设置clustername 1，参数指定 nodeBuilder().clusterName("yourclustername")
		 * 2，在classpath下面设置yml的配置文件
		 **/
		Node node = nodeBuilder().clusterName(clusterName).client(true).node();
		Client client = node.client();
		return client;
	}

	/**
	 * TransportClient的构建方式，不加入集群，只与集群中的一两个节点相连（round_robin）
	 * 指定集群也可采用参数指定和配置文件两种方式 其它可用参数： client.transport.ignore_cluster_name
	 * client.transport.ping_timeout client.transport.nodes_sampler_interval
	 * 
	 * @return
	 */
	public Client transportClient() {
		Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
		TransportClient client = TransportClient.builder().settings(settings).build();
		for (String address : addressArray) {
			client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(address, port)));
		}
		return client;
	}

}
