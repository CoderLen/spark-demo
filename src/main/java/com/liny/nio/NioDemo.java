package com.liny.nio;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

public class NioDemo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FileInputStream fin = new FileInputStream("douban.txt");
		Charset latin1 = Charset.forName("utf8");
		CharsetDecoder decoder = latin1.newDecoder();
		CharsetEncoder encoder = latin1.newEncoder();
		FileChannel fc = fin.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(128);
		int bytesRead = fc.read(buffer);
		while (bytesRead != -1) {
			while (buffer.hasRemaining()) {
				System.out.print((char) buffer.get());
			}
			buffer.clear();
			bytesRead = fc.read(buffer);
		}
		fc.close();
		fin.close();
	}

}
