package com.segmeno.kodo.database;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.segmeno.kodo.entity.Role;
import com.segmeno.kodo.entity.User;
import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.OperatorId;

public class DataAccessManagerTest {

	private static final Logger LOG = LogManager.getLogger(DataAccessManagerTest.class);
	private static DataAccessManager manager;
	private static Connection con;
	private static JdbcDataSource ds;

	@BeforeClass
	public static void setup() throws Exception {
		LOG.info("initializing MySql Tests");

		Class.forName("org.h2.Driver");
		ds = new JdbcDataSource();
		ds.setUrl("jdbc:h2:mem:testcase;MODE=MYSQL");
		ds.setUser("sa");

		manager = new DataAccessManager(ds);

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

//	@Test
//	@Transactional(propagation = Propagation.REQUIRED)
//	public void crudTest() throws Exception {
//		User user = new User();
//		user.name = "junit";
//		user.passwordHash = "123";
//		user.createdAt = new Date();
//
//		manager.addElem(user);
//		List<User> users = manager.getElems(new Criteria("name", OperatorId.EQUALS, "junit"), User.class);
//		assertTrue(users.size() == 1);
//		final User insertedUser = users.get(0);
//		assertTrue(insertedUser.addresses.isEmpty());
//		assertTrue(insertedUser.roles.isEmpty());
//
//		insertedUser.name = "junitTest";
//		manager.updateElem(insertedUser);
//		users = manager.getElems(new Criteria("name", OperatorId.EQUALS, "junitTest"), User.class);
//		assertTrue(users.size() == 1);
//
//		manager.deleteElem(insertedUser);
//		users = manager.getElems(new Criteria("name", OperatorId.EQUALS, "junitTest"), User.class);
//		assertTrue(users.size() == 0);
//	}

	@Test
	public void countElemTest() throws Exception {
		long count = manager.getElemCount(User.class);
		assertTrue(count == 2);
	}

	@Test
	public void getElemsTest() throws Exception {

		final List<User> users = manager.getElems(User.class);
		assertTrue(users.size() == 2);

		final User tom = users.stream().filter(user -> user.name.equals("Tom")).findFirst().orElse(null);
		assertNotNull(tom);
		assertTrue(tom.addresses.size() == 2);
		assertTrue(tom.roles.size() == 2);

		final User tim = users.stream().filter(user -> user.name.equals("Tim")).findFirst().orElse(null);
		assertNotNull(tim);
		assertTrue(tim.addresses.size() == 1);
		assertTrue(tim.roles.size() == 1);
		
		final Role timsRole = tim.roles.get(0);
		assertTrue(timsRole.primaryColor.name.equals("green"));
		assertTrue(timsRole.secondaryColor.name.equals("green"));
	}

	@Test
	public void buildQueryTest() throws Exception {

			final List<Object> params = new ArrayList<Object>();
			
			final AdvancedCriteria filter = new AdvancedCriteria(OperatorId.AND)
					.add(new Criteria("Name", OperatorId.ENDS_WITH, "m"));
					
			final String query = manager.buildQuery(new User(), filter, params);

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
