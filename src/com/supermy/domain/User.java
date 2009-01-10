package com.supermy.domain;

import java.util.Map;

import com.supermy.annotation.Column;
import com.supermy.annotation.Table;

@Table(name = "user_test")
public class User extends Base {

	@Column(name = "name", bloomfilter = true)
	private String name;
	@Column(name = "sex")
	private String sex;
	@Column(name = "age")
	private int age;
	@Column(name = "contact")
	private Map<String, String> contact;

	public String toString() {
		if (name == null) {
			return "";
		}
		StringBuffer sb = new StringBuffer(name);
		sb.append(";").append(sex).append(";").append(age).append(";").append(
				contact + " createtime:" + getCreateTime());
		return sb.toString();

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
	public Map<String, String> getContact() {
		return contact;
	}

	/**
	 * @param contact
	 *            the contact to set
	 */
	public void setContact(Map<String, String> contact) {
		this.contact = contact;
	}

}
