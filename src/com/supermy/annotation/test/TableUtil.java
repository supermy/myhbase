package com.supermy.annotation.test;

public class TableUtil {
	static public String getTable(Class<?> clazz) {
		return clazz.getName().replace(".", "_");
	}

}
