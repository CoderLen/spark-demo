package com.liny.crawler.thread;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liny.elastic2.util.ElasticSearchUtil;
import com.liny.kafka.ConsumerMsgTask;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerThread<T> extends Thread {

	private final ConsumerConnector consumer;
	private final String topic;
	private final int numThreads = 10;
	private ExecutorService executor;
	private String indexName;

	public ConsumerThread(String _zookeeper, String _group, String _topic, String _indexName) {
		this.topic = _topic;
		this.indexName = _indexName;
		ElasticSearchUtil.init();
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(_zookeeper, _group));
	}

	private static ConsumerConfig createConsumerConfig(String _zookeeper, String _groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", _zookeeper);
		props.put("group.id", _groupId);
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		// now launch all the threads
		executor = Executors.newFixedThreadPool(numThreads);
		// now create an object to consume the messages
		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new ConsumerMsgTask<T>(stream, indexName, threadNumber));
			threadNumber++;
		}
	}
}
