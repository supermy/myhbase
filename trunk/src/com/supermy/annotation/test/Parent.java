package com.supermy.annotation.test;

public class Parent {

	public static void test() {
		StackTraceElement[] stackTraceElement = new Throwable().getStackTrace();
		System.out.println(stackTraceElement[stackTraceElement.length - 1]
				.getClassName());

	}

	public static void main(String[] args) {
		Parent.test();
	}

}