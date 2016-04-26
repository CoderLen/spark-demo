package com.liny.crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;

public class PolicyCrawler extends BreadthCrawler {

	public BufferedWriter bw = null;

	public PolicyCrawler(String crawlPath, boolean autoParse) {
		super(crawlPath, autoParse);
		/* start page */
		this.addSeed("http://www.gdstc.gov.cn/zwgk/zcfg/gjzcfg/index@1.htm");

		/* fetch url like http://news.yahoo.com/xxxxx */
		this.addRegex("http://www.gdstc.gov.cn/HTML/zwgk/.*");

		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("policy.txt")), "utf-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void visit(Page page, CrawlDatums nextLinks) {
		// TODO Auto-generated method stub
		String url = page.getUrl();
		/* if page is news page */
		String question_regex = "http://www.gdstc.gov.cn/HTML/zwgk/.*";
		if (Pattern.matches(question_regex, url)) {
			/* we use jsoup to parse page */
			Document doc = page.getDoc();
			String title = doc.select("div.title").text();
			System.out.println("title:" + title);
			try {
				bw.write("标题:" + title.trim());
				bw.newLine();
				String time = doc.select("div[class=time]").text();
				bw.write("来源及时间:" + time.trim());
				bw.newLine();
				String content = doc.select("#gContent > div.content").text();
				bw.write("正文:" + content.trim());
				bw.newLine();
				Elements e1 = doc.select("img");
				for (Element e : e1) {
					String realUrl = e.attr("abs:src");
					bw.write("图片:" + realUrl);
					bw.newLine();
				}
				bw.write("原文地址:" + doc.baseUri());
				bw.newLine();
				bw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		PolicyCrawler crawler = new PolicyCrawler("policy", true);
		crawler.setThreads(20);
		crawler.setTopN(100);
		// crawler.setResumable(true);
		/* start crawl with depth of 4 */
		crawler.start(5);
	}
}
