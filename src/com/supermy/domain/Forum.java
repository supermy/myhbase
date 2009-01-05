package com.supermy.domain;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;
import com.supermy.annotation.Table;

@Table(name = "forum_test")
public class Forum {

	@ID("auto")
	public String id;
	@Column(name = "title", bloomfilter = true)
	private String title;
	@Column(name = "content")
	private String content;
	@Column(name = "publish_user")
	private String publishUser;
	@Column(name = "publish_time")
	private String publishTime;

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title
	 *            the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return the content
	 */
	public String getContent() {
		return content;
	}

	/**
	 * @param content
	 *            the content to set
	 */
	public void setContent(String content) {
		this.content = content;
	}

	/**
	 * @return the publishUser
	 */
	public String getPublishUser() {
		return publishUser;
	}

	/**
	 * @param publishUser
	 *            the publishUser to set
	 */
	public void setPublishUser(String publishUser) {
		this.publishUser = publishUser;
	}

	/**
	 * @return the publishTime
	 */
	public String getPublishTime() {
		return publishTime;
	}

	/**
	 * @param publishTime
	 *            the publishTime to set
	 */
	public void setPublishTime(String publishTime) {
		this.publishTime = publishTime;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
