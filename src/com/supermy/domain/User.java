package com.supermy.domain;

import java.util.Map;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;
import com.supermy.annotation.Table;
import com.supermy.annotation.ID.IdType;

@Table(name = "user_test")
public class User extends Action {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	@Column(name = "name", bloomfilter = true)
	private String name;
	@Column(name = "sex")
	private String sex;
	@Column(name = "age")
	private int age;
	@Column(name = "contact")
	private Map<String, Object> contact;
	public User() {
	}

	public User(String name) {
		this.name = name;
	}


	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the sex
	 */
	public String getSex() {
		return sex;
	}

	/**
	 * @param sex
	 *            the sex to set
	 */
	public void setSex(String sex) {
		this.sex = sex;
	}

	/**
	 * @return the age
	 */
	public int getAge() {
		return age;
	}

	/**
	 * @param age
	 *            the age to set
	 */
	public void setAge(int age) {
		this.age = age;
	}

	/**
	 * @return the contact
	 */
	public Map<String, Object> getContact() {
		return contact;
	}

	/**
	 * @param contact
	 *            the contact to set
	 */
	public void setContact(Map<String, Object> contact) {
		this.contact = contact;
	}

}
