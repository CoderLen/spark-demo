package com.liny.crawler.douban.bean;

import java.io.Serializable;

/**
 * 豆瓣影评
 * 
 * @author 泳
 * @date 2016年2月24日
 */
public class ReviewInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Long reviewId;

	private Long subjectId;

	private String title;

	private String url;

	private String content;

	private String userId;

	private String userName;

	private String time;

	private Integer useful;

	private Integer unuseful;

	public Long getReviewId() {
		return reviewId;
	}

	public void setReviewId(Long reviewId) {
		this.reviewId = reviewId;
	}

	public Long getSubjectId() {
		return subjectId;
	}

	public void setSubjectId(Long subjectId) {
		this.subjectId = subjectId;
	}

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

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public Integer getUseful() {
		return useful;
	}

	public void setUseful(Integer useful) {
		this.useful = useful;
	}

	public Integer getUnuseful() {
		return unuseful;
	}

	public void setUnuseful(Integer unuseful) {
		this.unuseful = unuseful;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
