package com.supermy.domain;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;

public abstract class Base implements Serializable{

	@ID
	private String id;
	
	@Column
	private Date createTime=new Date();;

	
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

	@Override
	public String toString() {
		return
		 ToStringBuilder.reflectionToString(this,ToStringStyle.MULTI_LINE_STYLE);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
