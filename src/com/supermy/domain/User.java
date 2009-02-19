package com.supermy.domain;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.supermy.annotation.Column;
import com.supermy.annotation.Table;
import com.supermy.annotation.test.Parent;
import com.supermy.utils.MD5;

@Table
public class User extends Action {

	public static void main(String[] args) {
		User.getClassName();
		User.getClassName1();
		new User.CurrentClassGetter().getClassName();
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Column(bloomfilter = true)
	private String email;
	@Column(bloomfilter = true)
	private String password;

	private String name;
	private String sex;
	private int age;
	private String address;
	private String zip;
	@Column
	private Map<String, Object> contact;
	@Column
	private boolean actived = false;

	public User() {
		
	}

	/**
	 * email  reverse 便于hbadoop存储和检索
	 * @param email
	 */
	public User(String email) {
		this.email = email;
		setId(MD5.getMD5(StringUtils.reverse(email).getBytes()));
	}

	/**
	 * @return the actived
	 */
	public boolean isActived() {
		return actived;
	}

	/**
	 * @param actived
	 *            the actived to set
	 */
	public void setActived(boolean actived) {
		this.actived = actived;
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

	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * @param email
	 *            the email to set
	 */
	public void setEmail(String email) {
		this.email = email;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the address
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @param address
	 *            the address to set
	 */
	public void setAddress(String address) {
		this.address = address;
	}

	/**
	 * @return the zip
	 */
	public String getZip() {
		return zip;
	}

	/**
	 * @param zip
	 *            the zip to set
	 */
	public void setZip(String zip) {
		this.zip = zip;
	}

}
