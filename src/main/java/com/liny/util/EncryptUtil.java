package com.liny.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class EncryptUtil {

	private static final byte[] DESKEY = "Systex!@#".getBytes();

	@SuppressWarnings("restriction")
	private static BASE64Decoder base64Decoder = new BASE64Decoder();

	@SuppressWarnings("restriction")
	private static BASE64Encoder base64Encoder = new BASE64Encoder();

	/**
	 * Encodes a string
	 *
	 * MD5 32位 小写 加密
	 *
	 * @param str
	 *            String to encode
	 * @return Encoded String
	 * @throws NoSuchAlgorithmException
	 */
	public static String md5(String str) {
		if (str == null || str.length() == 0) {
			throw new IllegalArgumentException("String to encript cannot be null or zero length");
		}

		StringBuffer hexString = new StringBuffer();

		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes());
			byte[] hash = md.digest();

			for (int i = 0; i < hash.length; i++) {
				if ((0xff & hash[i]) < 0x10) {
					hexString.append("0" + Integer.toHexString((0xFF & hash[i])));
				} else {
					hexString.append(Integer.toHexString(0xFF & hash[i]));
				}
			}
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("" + e);
		}

		return hexString.toString();
	}

	/**
	 * DES加密
	 * 
	 * @param encryptData
	 * @return
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeySpecException
	 */
	public static String desEncrypt(String encryptData) throws InvalidKeyException, NoSuchAlgorithmException,
			IllegalBlockSizeException, BadPaddingException, NoSuchPaddingException, InvalidKeySpecException {
		// DES算法要求有一个可信任的随机数源
		SecureRandom sr = new SecureRandom();
		// 从原始密匙数据创建一个DESKeySpec对象
		DESKeySpec dks = new DESKeySpec(DESKEY);
		// 创建一个密匙工厂，然后用它把DESKeySpec转换成一个SecretKey对象
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
		SecretKey key = keyFactory.generateSecret(dks);
		// Cipher对象实际完成加密操作
		Cipher cipher = Cipher.getInstance("DES");
		// 用密匙初始化Cipher对象
		cipher.init(Cipher.ENCRYPT_MODE, key, sr);
		// 正式执行加密操作
		@SuppressWarnings("restriction")
		String encryptedData = base64Encoder.encode(cipher.doFinal(encryptData.getBytes()));
		return encryptedData;
	}

	/**
	 * DES解密
	 * 
	 * @param encryptedData
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeySpecException
	 */
	@SuppressWarnings("restriction")
	public static String desDecrypt(String encryptedData) throws IllegalBlockSizeException, BadPaddingException,
			InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeySpecException {
		// DES算法要求有一个可信任的随机数源
		SecureRandom sr = new SecureRandom();
		// 从原始密匙数据创建一个DESKeySpec对象
		DESKeySpec dks = new DESKeySpec(DESKEY);
		// 创建一个密匙工厂，然后用它把DESKeySpec对象转换成一个SecretKey对象
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
		SecretKey key = keyFactory.generateSecret(dks);
		// Cipher对象实际完成解密操作
		Cipher cipher = Cipher.getInstance("DES");
		// 用密匙初始化Cipher对象
		cipher.init(Cipher.DECRYPT_MODE, key, sr);
		// 正式执行解密操作
		byte decryptedData[];
		try {
			decryptedData = cipher.doFinal(base64Decoder.decodeBuffer(encryptedData));
		} catch (IOException e) {
			throw new RuntimeException("解密错误，错误信息：", e);
		}
		String res = new String(decryptedData);
		return res;
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException,
			InvalidKeySpecException, IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException {

		String str = "算法的独立性是通过定义密码服务类来获得。用户只需了解密码算法的概念,而不用去关心如何实现这些概念。\n 实现的独立性和相互作用性通过密码服务提供器来实现。密码服务提供器是实现一个或多个密码服务的一个或多个程序包。软件开发商根据一定接口,将各种算法实现后,打包成一个提供器,用户可以安装不同的提供器。安装和配置提供器,可将包含提供器的ZIP和JAR文件放在CLASSPATH下,再编辑Java安全属性文件来设置定义一个提供器。"; // 待加密数据
		// 2.1 >>> 调用加密方法
		String encryptedData = desEncrypt(str);
		System.out.println("加密前：" + encryptedData);
		// 3.1 >>> 调用解密方法
		String decryptedData = desDecrypt(encryptedData);
		System.out.println("加密后：" + decryptedData);
	}

}
