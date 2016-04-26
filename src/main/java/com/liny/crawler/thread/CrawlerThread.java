package com.liny.crawler.thread;

import cn.edu.hfut.dmic.webcollector.crawler.Crawler;

public class CrawlerThread extends Thread {

	private Crawler crawler = null;

	public CrawlerThread(Crawler _crawler) {
		this.crawler = _crawler;
	}

	public void run() {
		crawler.setThreads(20);
		crawler.setTopN(20000);
		try {
			crawler.start(15);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
