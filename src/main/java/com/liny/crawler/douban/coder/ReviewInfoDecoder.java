package com.liny.crawler.douban.coder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.liny.crawler.douban.bean.ReviewInfo;

import kafka.serializer.Decoder;

public class ReviewInfoDecoder implements Decoder<ReviewInfo> {

	@Override
	public ReviewInfo fromBytes(byte[] bytes) {
		ReviewInfo pageData = null;
		ByteArrayInputStream bi = null;
		ObjectInputStream oi = null;
		try {
			bi = new ByteArrayInputStream(bytes);
			oi = new ObjectInputStream(bi);
			pageData = (ReviewInfo) oi.readObject();
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
