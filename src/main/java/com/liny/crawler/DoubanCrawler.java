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

public class DoubanCrawler extends BreadthCrawler {

	public BufferedWriter bw = null;

	public DoubanCrawler(String crawlPath, boolean autoParse) {
		super(crawlPath, autoParse);
		this.addSeed("http://movie.douban.com/subject/26728787/");

		/* fetch url like http://news.yahoo.com/xxxxx */
		this.addRegex("http://movie.douban.com/.*");
		
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("douban2016.txt")), "utf-8"));
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
		String question_regex = "http://movie.douban.com/.*";
		String url = page.getUrl();
		if (Pattern.matches(question_regex, url)) {
			/* we use jsoup to parse page */
			Document doc = page.doc();

			String title = doc.select("#content > h1:nth-child(1)").text();
			System.out.println("title:" + title);
			try {
				bw.write("[title]:" + title);
				bw.newLine();
				bw.write("[url]:"+doc.baseUri());
				bw.newLine();
				Elements author = doc.select("div.main-hd > p:nth-child(1) > a:nth-child(2)");
				String peopleUrl = author.get(0).attr("href");
				String peopleName = author.get(0).text();
				bw.write("[author]:"+peopleUrl + " / " + peopleName);
				bw.newLine();
				bw.write("[time]:"+doc.select("div.main-hd > p:nth-child(1) > span.main-meta").text());
				bw.newLine();
				bw.write("[subject]:"+doc.select("#fixedInfo > div.side-back > a").text());
				bw.newLine();
				bw.write("[movie-summary]:"+doc.select("#movie-summary > ul").text());
				bw.newLine();
				bw.write("[movie-nav]:"+doc.select("#movie-summary > div.movie-nav").text());
				bw.newLine();
				String content = doc.select("#link-report > div:nth-child(1)").text();
				bw.write("[content]:" + content.trim());
				bw.newLine();
				bw.write("[useful]:"+doc.select("div.main-panel-useful > span:nth-child(1)").text());
				bw.newLine();
				bw.write("[unuseful]:"+doc.select("div.main-panel-useful > span:nth-child(2)").text());
				bw.newLine();
				bw.write("------------------------------------------------------------------------------------");
				bw.newLine();
				bw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		DoubanCrawler crawler = new DoubanCrawler("douban", true);
		crawler.setThreads(20);
		crawler.setTopN(100);
		// crawler.setResumable(true);
		/* start crawl with depth of 4 */
		crawler.start(4);
	}
}
