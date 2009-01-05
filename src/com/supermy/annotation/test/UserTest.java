/**
 * 
 */
package com.supermy.annotation.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.supermy.annotation.factory.MyHBaseFactory;
import com.supermy.domain.User;

/**
 * @author my
 * 
 */
public class UserTest {
	private static final Log log = LogFactory.getLog(UserTest.class);

	MyHBaseFactory fac = new MyHBaseFactory();

	@Test
	public void autoCreateHbase() throws ClassNotFoundException {
		fac.createMyHBae(User.class.getPackage().getName());

	}


	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

}
