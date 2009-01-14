/**
 * 
 */
package com.supermy.manager.test;


import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tv.movo.exception.MokiException;

import com.supermy.domain.User;
import com.supermy.manager.MyHBaseTemplate;

/**
 * @author my
 *
 */
public class MyHBaseTemplateTest {
	private static final Log log=LogFactory.getLog(MyHBaseTemplateTest.class);
	MyHBaseTemplate template;
	User newu;
		
	@Test
	public void findUserByName() throws MokiException{
		log.debug("find user by name ... ...");
		String name="tiger";
		//返回一个对象
		User user = template.findUserByName(name);
		log.debug(user);
		
		
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
		template=new MyHBaseTemplate();
//
//		newu = new User("tiger");
//		//newu.setId("xyz");
//		newu.setAge(33);
//		Map<String, Object> contact = new HashMap<String, Object>();
//		contact.put("contact", "中关村");
//		contact.put("email", "yy@yy.com");
//		contact.put("qq", "123456");
//		contact.put("msn", "msn@msn.com");
//		contact.put("test1", 12);
//		contact.put("test2", Long.parseLong("123456"));
//		Date value = new Date();
//		contact.put("test3", value);
//
//		newu.setContact(contact);
//		newu.setSex("男");
//		newu.saveOrUpdate();
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		//newu.delete();
	}

}
