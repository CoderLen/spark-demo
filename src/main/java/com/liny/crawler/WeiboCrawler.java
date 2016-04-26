package com.liny.crawler;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;

public class WeiboCrawler  extends BreadthCrawler {

	public WeiboCrawler(String crawlPath, boolean autoParse) {
		super(crawlPath, autoParse);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void visit(Page page, CrawlDatums arg1) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		  /*抽取微博*/  
      Elements weibos=page.doc().select("div.c");  
      for(Element weibo:weibos){  
          System.out.println(weibo.text());  
      }  
      /*如果要爬取评论，这里可以抽取评论页面的URL，返回*/  
	}

}
