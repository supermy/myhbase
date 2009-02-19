package com.supermy.domain;

import com.supermy.annotation.Column;
import com.supermy.annotation.Table;

/**
 * @author my
 *         HBase主键构造
 */
@Table( inMemory = true)
public class Primarykey {
	@Column( bloomfilter = true)
	private String primarykey;

	/**
	 * @return the primarykey
	 */
	public String getPrimarykey() {
		return primarykey;
	}

	/**
	 * @param primarykey
	 *            the primarykey to set
	 */
	public void setPrimarykey(String primarykey) {
		this.primarykey = primarykey;
	}

	
}
