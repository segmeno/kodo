package com.segmeno.kodo.database.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.segmeno.kodo.database.DatabaseEntity;
import com.segmeno.kodo.database.Role;
import com.segmeno.kodo.database.User;
import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.OperatorId;

public class MySqlManagerTest {

	protected static final Logger LOG = LogManager.getLogger(MySqlManagerTest.class);
	public static MySqlManager manager;
	
	private final static String DB_URL = "";
	private final static String DB_USER = "";
	private final static String DB_PW = "";
	@BeforeClass
	public static void init() throws Exception {
		LOG.info("initializing MySql tests");
		DriverManager.registerDriver((Driver) Class.forName("com.mysql.cj.jdbc.Driver").newInstance());
		final String url = DB_URL;
		
		final MysqlDataSource mySqlDs = new MysqlDataSource();
		mySqlDs.setUrl(url);
		mySqlDs.setUser(DB_USER);
		mySqlDs.setPassword(DB_PW);
		manager = new MySqlManager(mySqlDs);
	}
	
	@Test
	public void getSelectQueryTest() throws Exception {
		
		final Criteria c1 = new Criteria("ID", OperatorId.EQUALS, "1");
		final Criteria c2 = new Criteria("UserSign", OperatorId.ICONTAINS, "s");
		final List<Criteria> list1 = new ArrayList<Criteria>();
		list1.add(c1);
		list1.add(c2);
		
		final Criteria c3 = new Criteria("ID", OperatorId.EQUALS, "2");
		final Criteria c4 = new Criteria("RoleName", OperatorId.ICONTAINS, "x");
		final List<Criteria> list2 = new ArrayList<Criteria>();
		list2.add(c3);
		list2.add(c4);
		
		final User user = new User();
		user.advancedCriteria = new AdvancedCriteria(OperatorId.AND, list1);
		
		final Role role = new Role();
		role.advancedCriteria = new AdvancedCriteria(OperatorId.AND, list2);
		
		final List<DatabaseEntity> children = new ArrayList<>();
		children.add(role);
		
		final String expectedQuery =
				"SELECT tbUser.id AS tbUser_000_id, tbUser.userFirstName AS tbUser_000_userFirstName, " +
				"tbUser.userLastName AS tbUser_000_userLastName, tbUser.userSign AS tbUser_000_userSign, " +
				"tbRole.id AS tbRole_000_id, tbRole.roleName AS tbRole_000_roleName, tbRole.description AS tbRole_000_description " + 
				"FROM tbUser LEFT JOIN tbRole ON tbRole.id = tbUser.id " +
				"WHERE (tbUser.ID = '1' and tbUser.UserSign LIKE '%s%') AND " +
				"(tbRole.ID = '2' and tbRole.RoleName LIKE '%x%')";
		
		assertEquals("returned query does not match expected one",
				expectedQuery,
				manager.getSelectQuery(user, children));
	}

	@Test
	public void getCountQueryTest() throws Exception {
		assertEquals("returned query does not match expected one", 
				"SELECT COUNT(*) FROM (SELECT * FROM tbTest WHERE 1 = 1)", 
				manager.getCountQuery("SELECT * FROM tbTest WHERE 1 = 1"));
	}
	
	@Test
	public void getUpdateQueryTest() throws Exception {
		assertEquals("returned query does not match expected one", 
						"UPDATE tbTest SET a = :1, b = :2 WHERE TestID = :TestID", 
						manager.getUpdateQuery("tbTest", "a = :1, b = :2", "TestID"));
	}
	
	/**** THE TESTS BELOW REQUIRE AN EXISTING MYSQL CONNECTION ****/
//	
//	@Test
//	@Transactional(propagation = Propagation.REQUIRED)
//	public void crudTest() throws Exception {
//		
//		User user = new User();
//		user.userFirstName = "junit";
//		user.userLastName = "test";
//		user.userPassword = "123";
//		user.userSign = "junit test";
//		user.createdBy = "junit";
//		user.modifiedBy = "junit";
//		user.flag = 1;
//		
//		user = manager.addElem(user);
//		List<User> users = manager.getElems(new Criteria("UserSign", OperatorId.EQUALS, "junit test"), User.class);
//		assertTrue(users.size() == 1);
//		
//		user.userSign = "junitTest";
//		manager.updateElem(user);
//		users = manager.getElems(new Criteria("UserSign", OperatorId.EQUALS, "junitTest"), User.class);
//		assertTrue(users.size() == 1);
//		
//		manager.deleteElem(user);
//		users = manager.getElems(new Criteria("UserSign", OperatorId.EQUALS, "junitTest"), User.class);
//		assertTrue(users.size() == 0);
//	}
//	
//	@Test
//	public void getElemsTest() throws Exception {
//
//		final Map<Class<? extends DatabaseEntity>, AdvancedCriteria> filterByType = new HashMap<>();
//		
//		filterByType.put(User.class, new AdvancedCriteria(OperatorId.AND).
//						add(new Criteria("userSign", OperatorId.ICONTAINS, "seg")));
//		
//		filterByType.put(Role.class, new AdvancedCriteria(OperatorId.AND).
//						add(new Criteria("ID", OperatorId.NOT_NULL)));
//		
//		try {
//			List<?> entities = manager.getElems(filterByType, User.class);
//			System.out.println("found " + entities.size() + " entities");
//		} catch (Exception e) {
//			System.err.println("error during testing: " + e.getMessage());
//		}
//	}
//	
//	@Test
//	public void updateElemTest() throws Exception {
//		final User user = new User();
//		user.id = 5;
//		user.userSign = "TDR";
//		user.userFirstName = "Hans";
//		user.userLastName = "Wurst";
//		
//		final Role r = new Role();
//		r.id = 23;
//		r.roleName = "FullAccess";
//		r.description = "full access role";
//		
//		user.roles.add(r);
//		
//		manager.updateElem(user);
//	}
	
}
