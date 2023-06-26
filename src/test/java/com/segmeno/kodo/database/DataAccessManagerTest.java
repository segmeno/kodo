package com.segmeno.kodo.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
@TestMethodOrder(OrderAnnotation.class)
public class DataAccessManagerTest {
	private static final String ROLE_ADMIN = "Admin";
	private static final String ROLE_TESTER = "Tester";
	private static final String ROLE_NORMAL_GUY = "Normal Guy";
	private static final Logger LOG = LogManager.getLogger(DataAccessManagerTest.class);
	private static DataAccessManager manager;
	private static Connection con;
	private static JdbcDataSource ds;
	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	@BeforeAll
	public static void setup() throws Exception {
		LOG.info("initializing MySql Tests");

		Class.forName("org.h2.Driver");
		ds = new JdbcDataSource();
		ds.setUrl("jdbc:h2:mem:testcase;MODE=MYSQL");
		ds.setUser("sa");
		final JdbcTemplate templ = new JdbcTemplate(ds);

		manager = new DataAccessManager(templ);

		// prepare the test tables
		con = ds.getConnection();
		final Statement stmt = con.createStatement();

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
		stmt.execute("insert into tbRole (name, primaryColorId, secondaryColorId, description, createdAt) values ('" + ROLE_ADMIN + "', 1, 2, 'the admin role', '2020-01-01')");
		stmt.execute("insert into tbRole (name, primaryColorId, secondaryColorId, description, createdAt) values ('" + ROLE_TESTER+ "', 2, 2, 'the tester role', '2020-05-15')");
		stmt.execute("insert into tbRole (name, primaryColorId, secondaryColorId, description, createdAt) values ('" + ROLE_NORMAL_GUY + "', 3, 3, 'the user role', '2020-05-15')");
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
	@Order(1)
	public void dateBetweenTest() throws Exception {
		final Date from = DATE_FORMAT.parse("2019-01-01");
		final Date to = DATE_FORMAT.parse("2020-05-05");
		final List<Date> dates = new ArrayList<>();
		dates.add(from);
		dates.add(to);
		Criteria c = new Criteria("createdAt", Operator.BETWEEN, dates);
		List<TestUser> users = manager.getElems(c, TestUser.class, 0);
		assertTrue(users.size() == 1);
		assertTrue(users.get(0).name.equals("Tom"));

		final String fromStr = "2019-01-01";
		final String fromTo = "2020-05-05";
		final List<String> dateStrs = new ArrayList<>();
		dateStrs.add(fromStr);
		dateStrs.add(fromTo);
		c = new Criteria("createdAt", Operator.BETWEEN, dateStrs);
		users = manager.getElems(c, TestUser.class, 0);
		assertTrue(users.size() == 1);
		assertTrue(users.get(0).name.equals("Tom"));
	}

	@Test
    @Order(2)
    public void pkQueryTest() throws Exception {
        final ArrayList<Object> params = new ArrayList<>();
        params.add(ROLE_ADMIN);

        final List<TestUser> users = manager.getElemsByPkQuery("SELECT UserId FROM tbuserRole WHERE RoleId IN (SELECT id FROM tbRole WHERE Name = ?)", params, TestUser.class);
        assertTrue(users.size() == 1);
        assertTrue(users.get(0).name.equals("Tom"));
    }

	@Test
    @Order(3)
	public void countElemTest() throws Exception {
		final long count = manager.getElemCount(TestUser.class);
		assertTrue(count == 2);
	}

	@Test
    @Order(4)
	public void customSqlTest() throws Exception {
		final List<CustomElement> customs = manager.getElems(CustomElement.class);
		assertTrue(customs.size() == 2);
	}

	@Test
    @Order(5)
	public void sortTest() throws Exception {
		List<TestUser> users = manager.getElems(null, TestUser.class, new Sort("tbUser.Name", SortDirection.ASC), -1);
		assertTrue(users.get(0).name.equalsIgnoreCase("Tim"));

		users = manager.getElems(null, TestUser.class, new Sort("tbUser.Name", SortDirection.DESC), -1);
		assertTrue(users.get(0).name.equalsIgnoreCase("Tom"));
	}

    @Test
    @Order(6)
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
    @Order(7)
	public void updateElemTest() throws Exception {
		Criteria c = new Criteria("name", Operator.EQUALS, "Bill");
		TestUser u = new TestUser();
		u.name = "Bill";
		u.pwHash = "ttt";
		
		
		TestRole r1 = new TestRole();
		r1.name = ROLE_ADMIN;
		r1.id = 1;
		TestRole r2 = new TestRole();
		r2.name = ROLE_TESTER;
		r2.id = 2;
		
		u.roles.add(r1);
		u.roles.add(r2);
		
		// create
		u = manager.addElem(u);
		try {		
			// check
			List<TestUser> l = manager.getElems(c, TestUser.class);
			
			assertNotNull(l);
			assertEquals(l.size(), 1);
			u = l.get(0);
			assertEquals(u.pwHash, "ttt");
			assertNotNull(u.roles);
			assertEquals(u.roles.size(), 2);
			assertNotNull(u.roles.stream().filter(r -> r.name.equals(ROLE_ADMIN)).findFirst().orElse(null));
			assertNotNull(u.roles.stream().filter(r -> r.name.equals(ROLE_TESTER)).findFirst().orElse(null));
			
			// modify
			u.pwHash = "ttt2";
			u.roles = u.roles.stream().filter(r -> {
				// remove ROLE_TESTER
				return r.name.equals(ROLE_ADMIN);
			}).collect(Collectors.toList());
			TestRole r3 = new TestRole();
			r3.name = ROLE_NORMAL_GUY;
			r3.id = 3;
			u.roles.add(r3);
					
			manager.updateElem(u);
			
			// check
			l = manager.getElems(c, TestUser.class);
			assertNotNull(l);
			assertEquals(l.size(), 1);
			u = l.get(0);
			assertEquals(u.pwHash, "ttt2");
			assertNotNull(u.roles);
			assertEquals(u.roles.size(), 2);
			assertNotNull(u.roles.stream().filter(r -> r.name.equals(ROLE_ADMIN)).findFirst().orElse(null));
			assertNotNull(u.roles.stream().filter(r -> r.name.equals(ROLE_NORMAL_GUY)).findFirst().orElse(null));
		} finally {
			// delete
			manager.deleteElems(c, TestUser.class);
		}
	}

	@Test
    @Order(8)
	public void getRecordsTest() throws Exception {
		final List<Map<String,Object>> res = manager.getRecords("tbUser", null, 10, 1, new Sort("Name", SortDirection.ASC));
		assertEquals(res.size(), 2);
		assertEquals((int)res.get(0).get("ID"), 2);
	}

	@Test
    @Order(9)
	public void getElemsTest() throws Exception {

		final List<TestUser> users = manager.getElems(TestUser.class);
		assertEquals(users.size(), 2);

		final TestUser tom = users.stream().filter(user -> user.name.equals("Tom")).findFirst().orElse(null);
		assertNotNull(tom);
		assertEquals(tom.addresses.size(), 2);
		assertEquals(tom.roles.size(), 2);

		final TestUser tim = users.stream().filter(user -> user.name.equals("Tim")).findFirst().orElse(null);
		assertNotNull(tim);
		assertEquals(tim.addresses.size(), 1);
		assertEquals(tim.roles.size(), 1);

		final TestRole timsRole = tim.roles.get(0);
		assertEquals(timsRole.primaryColor.name, ("green"));
		assertEquals(timsRole.secondaryColor.name, ("green"));
	}

	@Test
    @Order(10)
	public void deleteElemsTest() throws Exception {
		final CriteriaGroup crits = new CriteriaGroup(Operator.AND, new Criteria("Name", Operator.EQUALS, "Tom"));
		final TestUser user = (TestUser)manager.getElems(crits, TestUser.class).get(0);

		manager.deleteElems(crits, TestUser.class);

		assertTrue(manager.getElemCount(new Criteria("Name", Operator.EQUALS, "Tom"), TestUser.class) == 0);

		assertTrue(manager.getElemCount(TestRole.class) == 2);

		final CriteriaGroup cg = new CriteriaGroup(Operator.OR)
							.add(new Criteria("Street", Operator.EQUALS, "Elmstreet"))
							.add(new Criteria("Street", Operator.EQUALS, "Testplace"));

		assertTrue(manager.getElemCount(cg, TestAddress.class) == 0);

		manager.addElem(user);
	}

	@Test
    @Order(11)
	public void buildQueryTest() throws Exception {

			final ArrayList<Object> params = new ArrayList<Object>();

			final CriteriaGroup filter = new CriteriaGroup(Operator.AND)
					.add(new Criteria("Name", Operator.ENDS_WITH, "m"));

			final String query = manager.buildQuery(new TestUser(), filter, params);

			System.out.println(DataAccessManager.sqlPrettyPrint(query.toString()) + "\t" + params + "\n");
			executeAndPrintResults(query.toString(), params.toArray());
	}

	private void executeAndPrintResults(final String query, final Object[] args) throws SQLException {
		final JdbcTemplate template = new JdbcTemplate(ds);
		final StringBuilder sb = new StringBuilder();
		final AtomicBoolean headersPrinted = new AtomicBoolean(false);

		template.query(query, new RowCallbackHandler() {
			@Override
			public void processRow(final ResultSet rs) throws SQLException {
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

	private String fixedLength(String s, final int length) {
	    s = String.valueOf(s);
		if (s.length() >= length) {
			return s.substring(0, length);
		}
		while (s.length() < length) {
			s += " ";
		}
		return s;
	}

}
