package com.supermy.utils;

/**
 * @author my
 * 
 */
public class MyHBaseException extends Exception {

	private static final long serialVersionUID = 1L;

	public MyHBaseException() {
		super();
	}

	public MyHBaseException(String msg) {
		super(msg);
	}

	public MyHBaseException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public MyHBaseException(Throwable cause) {
		super(cause);
	}

}
