package com.supermy.domain;

import java.util.Date;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;

public class Base extends Action{

	@ID
	private String id;
	
	@Column(name="createtime")
	private Date createTime;

	
	/**
	 * @return the createTime
	 */
	public Date getCreateTime() {
		return createTime;
	}

	/**
	 * @param createTime
	 *            the createTime to set
	 */
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

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
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
