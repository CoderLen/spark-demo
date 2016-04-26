package com.liny.kafka.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.liny.crawler.bean.ZhihuData;

import kafka.serializer.Decoder;

public class ZhihuDataDecoder implements Decoder<ZhihuData> {

	@Override
	public ZhihuData fromBytes(byte[] bytes) {
		// TODO Auto-generated method stub
		ZhihuData pageData = null;
		ByteArrayInputStream bi = null;
		ObjectInputStream oi = null;
		try {
			bi = new ByteArrayInputStream(bytes);
			oi = new ObjectInputStream(bi);
			pageData = (ZhihuData) oi.readObject();
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
		return pageData;
	}

}
