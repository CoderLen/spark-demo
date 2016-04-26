package com.liny.crawler.thread;

import com.liny.crawler.ZhihuCrawler;
import com.liny.crawler.bean.ZhihuData;

import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;

public class Main {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String name = "zhihu_answer_20160426";
		String topic = name;
		String indexName = name;
		BreadthCrawler crawler = new ZhihuCrawler(topic, "crawls", true);
		/* start page */
//		crawler.addSeed("https://movie.douban.com/subject/26322928/reviews");
		crawler.addSeed("https://www.zhihu.com/question/44265033");
//		crawler.addRegex("http://movie.douban.com/review/.*");
		/* fetch url like http://news.yahoo.com/xxxxx */
		crawler.addRegex("http://www.zhihu.com/question/[0-9]+");
		CrawlerThread crawlerThread = new CrawlerThread(crawler);
		ConsumerThread<ZhihuData> consumerThread = new ConsumerThread<ZhihuData>("192.168.137.132:2181", "group_2", topic, indexName);
		crawlerThread.start();
		consumerThread.start();
	}
}
