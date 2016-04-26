package com.liny.crawler;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.liny.crawler.bean.ZhihuData;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Crawl news from yahoo news
 *
 * @author hu
 */
public class ZhihuCrawler extends BreadthCrawler {

	public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss");

	private static String dataPattern = "\\d{4}-\\d{1,2}-\\d{1,2}";

	private static Pattern datePattern = Pattern.compile(dataPattern);

	private static Pattern numPattern = Pattern.compile("\\d+");

	private String topic;

	public Producer<String, ZhihuData> producer = null;

	/**
	 * @param crawlPath
	 *            crawlPath is the path of the directory which maintains
	 *            information of this crawler
	 * @param autoParse
	 *            if autoParse is true,BreadthCrawler will auto extract links
	 *            which match regex rules from pag
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public ZhihuCrawler(String _topic, String crawlPath, boolean autoParse) {
		super(crawlPath, autoParse);

		this.topic = _topic;
		// 设置配置属性
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.137.132:9092");
		props.put("serializer.class", "com.liny.kafka.util.ZhihuDataEncoder");
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
		producer = new Producer<String, ZhihuData>(config);

	}

	@Override
	public void visit(Page page, CrawlDatums nextLinks) {
		String url = page.getUrl();
		ZhihuData zhihuData = new ZhihuData();
		zhihuData.setUrl(url);
		/* if page is news page */
		String question_regex = "^http://www.zhihu.com/question/[0-9]+";
		if (Pattern.matches(question_regex, url)) {
			/* we use jsoup to parse page */
			Document doc = page.doc();
			String title = doc.select("h2[class=zm-item-title zm-editable-content]").text();
			zhihuData.setTitle(title);
			System.out.println("title:" + title);

			Elements tags = doc.select("a[class=zm-item-tag]");
			StringBuffer tagStr = new StringBuffer();
			for (Element tag : tags) {
				if (tag != tags.last()) {
					tagStr.append(tag.text()).append(",");
				} else {
					tagStr.append(tag.text());
				}
			}
			zhihuData.setTags(tagStr.toString());
			String zhaiyao = doc.select("div[class=zm-editable-content]").text();
			zhihuData.setZhaiyao(zhaiyao);
			Elements answers = doc.select("div[class=zm-item-answer  zm-item-expanded]");
			for (Element el : answers) {
				String answerUser = el.select("a[class=author-link]").text();
				zhihuData.setAnswerUser(answerUser);
				String answerUserId = el.select("a[class=author-link]").attr("href");
				zhihuData.setAnswerUserId(answerUserId);
				String count = el.select("span[class=count]").text();
				if (count.contains("K")) {
					int temp = Integer.parseInt(count.replace("K", ""));
					zhihuData.setAgreeCount(temp * 1000);
				} else {
					zhihuData.setAgreeCount(Integer.parseInt(count));
				}
				String commentCountStr = el.select("a[name=addcomment]").text();
				Matcher mt = numPattern.matcher(commentCountStr);
				if (mt.find()) {
					String commentCount = mt.group();
					zhihuData.setCommentCount(Integer.parseInt(commentCount));
				}

				String content = el.select("div[class=zm-editable-content clearfix]").text();
				zhihuData.setContent(content);
				String answerDateText = el.select("span[class=answer-date-link-wrap]").text();
				Matcher matcher = datePattern.matcher(answerDateText);
				if (matcher.find()) {
					String answerDate = matcher.group();
					zhihuData.setAnswerDate(answerDate);
				}
				zhihuData.setCrawlerTime(format.format(new Date()));
				// System.out.println(zhihuData.toString());
				KeyedMessage<String, ZhihuData> message = new KeyedMessage<String, ZhihuData>(topic, url, zhihuData);
				producer.send(message);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ZhihuCrawler crawler = new ZhihuCrawler("", "crawl", true);
		crawler.setThreads(20);
		crawler.setTopN(100);
		crawler.start(15);
	}
}
