package com.liny.crawler.douban.bean;

import java.io.Serializable;

/**
 * 豆瓣电影
 * 
 * @author 泳
 * @date 2016年2月24日
 */
public class SubjectInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Long subjectId;

	private String name;

	private String url;

	private String director;

	private String screenwriter;

	private String style;

	private String actors;

	private String area;

	private String language;

	private String releaseDate;

	private String score;

	private String duration;

	public Long getSubjectId() {
		return subjectId;
	}

	public void setSubjectId(Long subjectId) {
		this.subjectId = subjectId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDirector() {
		return director;
	}

	public void setDirector(String director) {
		this.director = director;
	}

	public String getScreenwriter() {
		return screenwriter;
	}

	public void setScreenwriter(String screenwriter) {
		this.screenwriter = screenwriter;
	}

	public String getStyle() {
		return style;
	}

	public void setStyle(String style) {
		this.style = style;
	}

	public String getActors() {
		return actors;
	}

	public void setActors(String actors) {
		this.actors = actors;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getReleaseDate() {
		return releaseDate;
	}

	public void setReleaseDate(String releaseDate) {
		this.releaseDate = releaseDate;
	}

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

}
