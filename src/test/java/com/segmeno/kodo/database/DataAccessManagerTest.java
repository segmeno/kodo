package com.segmeno.kodo.database;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.segmeno.kodo.entity.CustomElement;
import com.segmeno.kodo.entity.TestAddress;
import com.segmeno.kodo.entity.TestRole;
import com.segmeno.kodo.entity.TestType;
import com.segmeno.kodo.entity.TestUser;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.Operator;
import com.segmeno.kodo.transport.Sort;
import com.segmeno.kodo.transport.Sort.SortDirection;

public class DataAccessManagerTest {

	private static final Logger LOG = LogManager.getLogger(DataAccessManagerTest.class);
	private static DataAccessManager manager;
	private static Connection con;
	private static JdbcDataSource ds;
	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	@BeforeClass
	public static void setup() throws Exception {
		LOG.info("initializing MySql Tests");

		Class.forName("org.h2.Driver");
		ds = new JdbcDataSource();
		ds.setUrl("jdbc:h2:mem:testcase;MODE=MYSQL");
		ds.setUser("sa");
		JdbcTemplate templ = new JdbcTemplate(ds);

		manager = new DataAccessManager(templ);

		// prepare the test tables
		con = ds.getConnection();
		Statement stmt = con.createStatement();

		stmt.execute("SET MODE MYSQL");
		con.commit();

		stmt.execute("create table tbUser (id integer AUTO_INCREMENT PRIMARY KEY, name varchar, passwordHash varchar, clearanceLevelId integer, createdAt timestamp)");
		stmt.execute("create table tbRole (id integer AUTO_INCREMENT PRIMARY KEY, primaryColorId integer, secondaryColorId integer, name varchar, description varchar, createdAt timestamp)");
		stmt.execute("create table tbUserRole (id integer AUTO_INCREMENT PRIMARY KEY, userId integer, roleId integer)");
		stmt.execute("create table tbAddress (id integer AUTO_INCREMENT PRIMARY KEY, userId integer, street varchar, postalCode varchar, createdAt timestamp)");
		stmt.execute("create table tbType (id integer AUTO_INCREMENT PRIMARY KEY, name varchar)");
		con.commit();

		stmt.execute("insert into tbType (name) values ('red'), ('green'), ('blue'), ('RESTRICTED'), ('ALL ACCESS')");
		con.commit();

		stmt.execute("insert into tbUser (name, passwordHash, clearanceLevelId, createdAt) values ('Tom', 'pw123', 5, '2020-01-01')");
		stmt.execute("insert into tbUser (name, passwordHash, clearanceLevelId, createdAt) values ('Tim', 'pw456', 4, '2020-12-31')");
		stmt.execute("insert into tbRole (name, primaryColorId, secondaryColorId, description, createdAt) values ('Admin', 1, 2, 'the admin role', '2020-01-01')");
		stmt.execute("insert into tbRole (name, primaryColorId, secondaryColorId, description, createdAt) values ('Tester', 2, 2, 'the tester role', '2020-05-15')");
		con.commit();

		stmt.execute("insert into tbAddress (userId, street, postalCode, createdAt) values ((SELECT id FROM tbUser WHERE Name = 'Tom'), 'Elmstreet', '31117', '2020-01-01')");
		stmt.execute("insert into tbAddress (userId, street, postalCode, createdAt) values ((SELECT id FROM tbUser WHERE Name = 'Tom'), 'Testplace', '66654', '2020-01-01')");
		stmt.execute("insert into tbAddress (userId, street, postalCode, createdAt) values ((SELECT id FROM tbUser WHERE Name = 'Tim'), 'Knight`s Road', 'S-10092', '2020-01-01')");

		// tom is admin and tester, tim is tester
		stmt.execute("insert into tbUserRole (userId, roleId) values ((SELECT id FROM tbUser WHERE Name = 'Tom'), (SELECT id FROM tbRole WHERE Name = 'Admin'))");
		stmt.execute("insert into tbUserRole (userId, roleId) values ((SELECT id FROM tbUser WHERE Name = 'Tom'), (SELECT id FROM tbRole WHERE Name = 'Tester'))");
		stmt.execute("insert into tbUserRole (userId, roleId) values ((SELECT id FROM tbUser WHERE Name = 'Tim'), (SELECT id FROM tbRole WHERE Name = 'Tester'))");

		con.commit();
	}
	
	@Test
	public void dateBetweenTest() throws Exception {
		Date from = DATE_FORMAT.parse("2019-01-01");
		Date to = DATE_FORMAT.parse("2020-05-05");
		List<Date> dates = new ArrayList<>();
		dates.add(from);
		dates.add(to);
		Criteria c = new Criteria("createdAt", Operator.BETWEEN, dates);
		List<TestUser> users = manager.getElems(c, TestUser.class, 0);
		assertTrue(users.size() == 1);
		assertTrue(users.get(0).name.equals("Tom"));
	}

	@Test
	public void countElemTest() throws Exception {
		long count = manager.getElemCount(TestUser.class);
		assertTrue(count == 2);
	}
	
	@Test
	public void customSqlTest() throws Exception {
		List<CustomElement> customs = manager.getElems(CustomElement.class);
		assertTrue(customs.size() == 2);
	}
	
	@Test
	public void sortTest() throws Exception {
		List<TestUser> users = manager.getElems(null, TestUser.class, new Sort("tbUser.Name", SortDirection.ASC), -1);
		assertTrue(users.get(0).name.equalsIgnoreCase("Tim"));
		
		users = manager.getElems(null, TestUser.class, new Sort("tbUser.Name", SortDirection.DESC), -1);
		assertTrue(users.get(0).name.equalsIgnoreCase("Tom"));
	}
	
	@Test
	public void updateElemTest() throws Exception {
		
		TestUser u = new TestUser();
		u.name = "Bill";
		u.pwHash = "ttt";
		
		u = manager.addElem(u);
		manager.updateElem(u);
		manager.deleteElems(new Criteria("name", Operator.EQUALS, "Bill"), TestUser.class);
	}
	
	@Test
	public void getRecordsTest() throws Exception {
		final List<Map<String,Object>> res = manager.getRecords("tbUser", null, 10, 1, new Sort("Name", SortDirection.ASC));
		assertTrue(res.size() == 2);
		assertTrue((int)res.get(0).get("ID") == 2);
	}

	@Test
	public void getElemsTest() throws Exception {

		final List<TestUser> users = manager.getElems(TestUser.class);
		assertTrue(users.size() == 2);

		final TestUser tom = users.stream().filter(user -> user.name.equals("Tom")).findFirst().orElse(null);
		assertNotNull(tom);
		assertTrue(tom.addresses.size() == 2);
		assertTrue(tom.roles.size() == 2);

		final TestUser tim = users.stream().filter(user -> user.name.equals("Tim")).findFirst().orElse(null);
		assertNotNull(tim);
		assertTrue(tim.addresses.size() == 1);
		assertTrue(tim.roles.size() == 1);
		
		final TestRole timsRole = tim.roles.get(0);
		assertTrue(timsRole.primaryColor.name.equals("green"));
		assertTrue(timsRole.secondaryColor.name.equals("green"));
	}
	
	@Test
	public void deleteElemsTest() throws Exception {
		final CriteriaGroup crits = new CriteriaGroup(Operator.AND, new Criteria("Name", Operator.EQUALS, "Tom"));
		final TestUser user = (TestUser)manager.getElems(crits, TestUser.class).get(0);
		
		manager.deleteElems(crits, TestUser.class);
		
		assertTrue(manager.getElemCount(new Criteria("Name", Operator.EQUALS, "Tom"), TestUser.class) == 0);
		
		assertTrue(manager.getElemCount(TestRole.class) == 2);
		
		CriteriaGroup cg = new CriteriaGroup(Operator.OR)
							.add(new Criteria("Street", Operator.EQUALS, "Elmstreet"))
							.add(new Criteria("Street", Operator.EQUALS, "Testplace"));
		
		assertTrue(manager.getElemCount(cg, TestAddress.class) == 0);
		
		manager.addElem(user);
	}
	
	@Test
	public void addElemTest() throws Exception {
		
		final TestAddress addr = new TestAddress();
		addr.postalCode = "666666";
		addr.street = "junit street";
		
		final TestType clearance = new TestType();
		clearance.name = "SPECIAL OPERATIONS";
		
		final TestUser user = new TestUser();
		user.name = "Ted";
		user.addresses.add(addr);
		user.clearanceLevel = clearance;
		
		manager.addElem(user);
		assertTrue(manager.getElemCount(TestUser.class) == 3);
		assertTrue(manager.getElemCount(TestAddress.class) == 4);
		assertTrue(manager.getElemCount(TestType.class) == 6);
		
		// cleanup
		manager.deleteElems(new Criteria("Name", Operator.EQUALS, "Ted"), TestUser.class);
	}

	@Test
	public void buildQueryTest() throws Exception {

			final ArrayList<Object> params = new ArrayList<Object>();
			
			final CriteriaGroup filter = new CriteriaGroup(Operator.AND)
					.add(new Criteria("Name", Operator.ENDS_WITH, "m"));
					
			final String query = manager.buildQuery(new TestUser(), filter, params);

			System.out.println(DataAccessManager.sqlPrettyPrint(query.toString()) + "\t" + params + "\n");
			executeAndPrintResults(query.toString(), params.toArray());
	}

	private void executeAndPrintResults(String query, Object[] args) throws SQLException {
		final JdbcTemplate template = new JdbcTemplate(ds);
		final StringBuilder sb = new StringBuilder();
		final AtomicBoolean headersPrinted = new AtomicBoolean(false);
		
		template.query(query, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				if (!headersPrinted.get()) {
					for (int column = 1; column <= rs.getMetaData().getColumnCount(); ++column) {
						sb.append(fixedLength(rs.getMetaData().getColumnName(column),10)).append("  ");
					}
					System.out.println(sb.toString());
					headersPrinted.set(true);
					sb.setLength(0);
				}
				for (int column = 1; column <= rs.getMetaData().getColumnCount(); ++column) {
					sb.append(fixedLength(rs.getString(column),10)).append("  ");
				}
				System.out.println(sb.toString());
				sb.setLength(0);
			}
		}, args);
	}
	
	private String fixedLength(String s, int length) {
		if (s.length() >= length) {
			return s.substring(0, length);
		}
		while (s.length() < length) {
			s += " ";
		}
		return s;
	}

}
