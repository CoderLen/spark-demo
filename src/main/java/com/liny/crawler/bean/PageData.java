package com.liny.crawler.bean;

import java.io.Serializable;

public class PageData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String title;

	private String url;

	private String zhaiyao;

	private String content;

	private String date;

	private String crawlerTime;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getZhaiyao() {
		return zhaiyao;
	}

	public void setZhaiyao(String zhaiyao) {
		this.zhaiyao = zhaiyao;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getCrawlerTime() {
		return crawlerTime;
	}

	public void setCrawlerTime(String crawlerTime) {
		this.crawlerTime = crawlerTime;
	}

	@Override
	public String toString() {
		return "PageData [title=" + title + ", url=" + url + ", zhaiyao=" + zhaiyao + ", content=" + content + ", date="
				+ date + ", crawlerTime=" + crawlerTime + "]";
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
