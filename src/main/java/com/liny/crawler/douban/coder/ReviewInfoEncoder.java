package com.liny.crawler.douban.coder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.liny.crawler.douban.bean.ReviewInfo;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class ReviewInfoEncoder implements Encoder<ReviewInfo> {

	public ReviewInfoEncoder(VerifiableProperties props) {

	}
	
	@Override
	public byte[] toBytes(ReviewInfo pageData) {
		// TODO Auto-generated method stub
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
