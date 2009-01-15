/**
 * 
 */
package com.supermy.manager.test;


import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tv.movo.utils.MD5;

import com.supermy.domain.Action;
import com.supermy.domain.User;
import com.supermy.manager.MyHBaseTemplate;
import com.supermy.utils.MyHBaseException;

/**
 * @author my
 *
 */
public class MyHBaseTemplateTest {
	private static final Log log=LogFactory.getLog(MyHBaseTemplateTest.class);
	MyHBaseTemplate template;
	User newu;

	@Test
	public void userRegister() throws MyHBaseException{
		User newuser = new User();
		newuser.setEmail("springclick@gmail.com");
		newuser.setPassword("111111");
		newuser.setId(MD5.getMD5("springclick@gmail.com".getBytes()));
		
		//email要唯一
		User userByEmail = template.getUserByEmail(newuser.getEmail());
		if (userByEmail!=null) {
			throw new MyHBaseException("用户已经存在");
		}
		newuser.saveOrUpdate();
		log.debug(newuser.get());
		
	}
	
	@Test
	public void findUserByName() throws MyHBaseException{
		log.debug("find user by name ... ...");
		String name="tiger";
		//返回一个对象
		User user = template.findUserByName(name);
		log.debug(user);
		
		
	}

	@Test
	public void TypeNull() throws MyHBaseException{
		log.debug("find user by name ... ...");
		User u=(User)null;
		String  s=(String)null;
		//List<Action> list=new ArrayList<Action>();
		//u=(User)list.get(0);
		Map<String ,Action > m=new HashMap<String, Action>();
		u=(User)m.get("");
 		Assert.assertTrue(true);
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
