package com.liny.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.alibaba.fastjson.JSONObject;
import com.liny.elastic2.util.ElasticSearchUtil;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerMsgTask<T> implements Runnable {

	public static final String type = "info";

	private KafkaStream<byte[], byte[]> m_stream;
	private int m_threadNumber;
	private String indexName;

	public ConsumerMsgTask(KafkaStream<byte[], byte[]> stream, String _indexName, int threadNumber) {
		m_threadNumber = threadNumber;
		m_stream = stream;
		indexName = _indexName;
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {
			T object = fromBytes(it.next().message());
			String jsonData = JSONObject.toJSONString(object);
			ElasticSearchUtil.insertDoc(indexName, type, jsonData);
		}
		System.out.println("Shutting down Thread: " + m_threadNumber);
	}

	public T fromBytes(byte[] bytes) {
		// TODO Auto-generated method stub
		T zhihuData = null;
		ByteArrayInputStream bi = null;
		ObjectInputStream oi = null;
		try {
			bi = new ByteArrayInputStream(bytes);
			oi = new ObjectInputStream(bi);
			zhihuData = (T) oi.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bi.close();
				oi.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return zhihuData;
	}
}
