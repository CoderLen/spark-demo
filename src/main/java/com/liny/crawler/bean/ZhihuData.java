package com.liny.crawler.bean;

/**
 * 知乎回答
 * @author 泳
 * @date 2016年2月24日
 */
public class ZhihuData extends PageData{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int agreeCount = 0;
	
	private String answerUser;
	
	private String answerUserId;
	
	private String answerDate;
	
	private String tags;
	
	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public String getAnswerUserId() {
		return answerUserId;
	}

	public void setAnswerUserId(String answerUserId) {
		this.answerUserId = answerUserId;
	}

	public String getAnswerDate() {
		return answerDate;
	}

	public void setAnswerDate(String answerDate) {
		this.answerDate = answerDate;
	}

	private int commentCount;

	public int getAgreeCount() {
		return agreeCount;
	}

	public void setAgreeCount(int agreeCount) {
		this.agreeCount = agreeCount;
	}

	public String getAnswerUser() {
		return answerUser;
	}

	public void setAnswerUser(String answerUser) {
		this.answerUser = answerUser;
	}

	public int getCommentCount() {
		return commentCount;
	}

	public void setCommentCount(int commentCount) {
		this.commentCount = commentCount;
	}

	@Override
	public String toString() {
		return "ZhihuData [agreeCount=" + agreeCount + ", answerUser=" + answerUser + ", answerUserId=" + answerUserId
				+ ", answerDate=" + answerDate + ", tags=" + tags + ", commentCount=" + commentCount + ", getTags()="
				+ getTags() + ", getAnswerUserId()=" + getAnswerUserId() + ", getAnswerDate()=" + getAnswerDate()
				+ ", getAgreeCount()=" + getAgreeCount() + ", getAnswerUser()=" + getAnswerUser()
				+ ", getCommentCount()=" + getCommentCount() + ", getTitle()=" + getTitle() + ", getUrl()=" + getUrl()
				+ ", getZhaiyao()=" + getZhaiyao() + ", getContent()=" + getContent() + ", getDate()=" + getDate()
				+ ", getCrawlerTime()=" + getCrawlerTime() + "]";
	}
}
