package com.liny.kafka.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.liny.crawler.bean.ZhihuData;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class ZhihuDataEncoder implements Encoder<ZhihuData> {

	public ZhihuDataEncoder(VerifiableProperties props) {

	}

	@Override
	public byte[] toBytes(ZhihuData pageData) {
		// TODO Auto-generated method stub
		if (pageData == null) {
			return null;
		} else {
			byte[] bytes = null;		
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(pageData);
				oos.flush();
				bytes = bos.toByteArray();
				oos.close();
				bos.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			return bytes;
		}
	}
}
