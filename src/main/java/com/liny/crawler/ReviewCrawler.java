package com.liny.crawler;

import java.io.BufferedWriter;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.liny.crawler.douban.bean.ReviewInfo;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ReviewCrawler extends BreadthCrawler {

	public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss");

	private static String dataPattern = "\\d{4}-\\d{1,2}-\\d{1,2}";

	private static Pattern datePattern = Pattern.compile(dataPattern);

	private static Pattern numPattern = Pattern.compile("\\d+");

	private String topic;

	public Producer<String, ReviewInfo> producer = null;

	public BufferedWriter bw = null;

	public ReviewCrawler(String _topic, String crawlPath, boolean autoParse) {
		super(crawlPath, autoParse);

		this.topic = _topic;
		// 设置配置属性
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.137.132:9092");
		props.put("serializer.class", "com.liny.crawler.douban.coder.ReviewInfoEncoder");
		// key.serializer.class默认为serializer.class
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		// 可选配置，如果不配置，则使用默认的partitioner
		// props.put("partitioner.class", "com.liny.kafka.PartitionerDemo");
		// 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
		// 值为0,1,-1,可以参考
		// http://kafka.apache.org/08/configuration.html
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);

		// 创建producer
		producer = new Producer<String, ReviewInfo>(config);

	}

	@Override
	public void visit(Page page, CrawlDatums nextLinks) {
		// TODO Auto-generated method stub
		String question_regex = "http://movie.douban.com/review/[0-9]+/.*";
		String url = page.getUrl();
		if (Pattern.matches(question_regex, url)) {
			ReviewInfo review = new ReviewInfo();
			/* we use jsoup to parse page */
			Document doc = page.doc();
			String title = doc.select("#content > h1:nth-child(1)").text();
			System.out.println("title:" + title);
			review.setTitle(title);
			review.setUrl(doc.baseUri());
			Elements author = doc.select("div.main-hd > p:nth-child(1) > a:nth-child(2)");
			if (author.size() > 0) {
				String peopleUrl = author.get(0).attr("href");
				String peopleName = author.get(0).text();
				Matcher m = numPattern.matcher(peopleUrl);
				if(m.find()){
					review.setUserId(m.group());
				}
				review.setUserName(peopleName);
			}
			String time = doc.select("div.main-hd > p:nth-child(1) > span.main-meta").text();
			review.setTime(time);
			String subjectUrl = doc.select("#fixedInfo > div.side-back > a").attr("href");
			Matcher subjectMat = numPattern.matcher(subjectUrl);
			if(subjectMat.find()){
				review.setSubjectId(Long.valueOf(subjectMat.group()));
			}
			String content = doc.select("#link-report > div:nth-child(1)").text();
			review.setContent(content);
			String useful = doc.select("div.main-panel-useful > span:nth-child(1)").text();
			Matcher m1 = numPattern.matcher(useful);
			if (m1.find()) {
				String usefulCount = m1.group();
				review.setUseful(Integer.parseInt(usefulCount));
			} else {
				review.setUseful(0);
			}
			String unuseful = doc.select("div.main-panel-useful > span:nth-child(2)").text();
			Matcher m2 = numPattern.matcher(unuseful);
			if (m2.find()) {
				String unusefulCount = m2.group();
				review.setUnuseful(Integer.parseInt(unusefulCount));
			} else {
				review.setUnuseful(0);
			}
			KeyedMessage<String, ReviewInfo> message = new KeyedMessage<String, ReviewInfo>(topic, url, review);
			producer.send(message);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ReviewCrawler crawler = new ReviewCrawler("", "douban", true);
		crawler.setThreads(20);
		crawler.setTopN(100);
		// crawler.setResumable(true);
		/* start crawl with depth of 4 */
		crawler.start(4);
	}
}
