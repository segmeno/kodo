package com.segmeno.kodo.database.mysql;

import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.OperatorId;

public class MySqlManagerTest {

	protected static final Logger LOG = LogManager.getLogger(MySqlManagerTest.class);
	public static MySqlManager manager;
	
	@BeforeClass
	public static void init() throws Exception {
		LOG.info("initializing MySql tests");
		DataSource ds = new DataSource() {
			@Override
			public PrintWriter getLogWriter() throws SQLException {
				return null;
			}

			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
			}

			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
			}
			@Override
			public int getLoginTimeout() throws SQLException {
				return 0;
			}
			@Override
			public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				return null;
			}
			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				return false;
			}
			@Override
			public Connection getConnection() throws SQLException {
				return null;
			}
			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return null;
			}
		};
		
		manager = new MySqlManager(ds);
	}

	@Test
	public void getSelectByPrimaryKeyQueryTest() {
		assertEquals("returned query does not match expected one", 
						"SELECT * FROM tbTest WHERE TestID = ?", 
						manager.getSelectByPrimaryKeyQuery("tbTest", "TestID"));
	}
	
	@Test
	public void getSelectByCriteriaQueryTest_1() throws Exception {
		assertEquals("returned query does not match expected one", 
						"SELECT * FROM tbTest", 
						manager.getSelectByCriteriaQuery("tbTest", null));
	}
	
	@Test
	public void getSelectByCriteriaQueryTest_2() throws Exception {
		final Criteria c1 = new Criteria("Field_A", OperatorId.ENDS_WITH, "SSS");
		final Criteria c2 = new Criteria("Field_B", OperatorId.ICONTAINS, "XXX");
		final List<Criteria> list = new ArrayList<Criteria>();
		list.add(c1);
		list.add(c2);
		
		final AdvancedCriteria ac = new AdvancedCriteria(OperatorId.AND, list);
		
		assertEquals("returned query does not match expected one", 
						"SELECT * FROM tbTest WHERE BINARY Field_A LIKE '%SSS' and Field_B LIKE '%XXX%'", 
						manager.getSelectByCriteriaQuery("tbTest", ac));
	}
	
	@Test
	public void getCountQueryTest_1() throws Exception {
		assertEquals("returned query does not match expected one", 
				"SELECT COUNT(*) FROM tbTest", 
				manager.getCountQuery("tbTest", null));
	}
	
	@Test
	public void getCountQueryTest_2() throws Exception {
		final Criteria c1 = new Criteria("Field_A", OperatorId.ENDS_WITH, "SSS");
		final Criteria c2 = new Criteria("Field_B", OperatorId.ICONTAINS, "XXX");
		final List<Criteria> list = new ArrayList<Criteria>();
		list.add(c1);
		list.add(c2);
		
		final AdvancedCriteria ac = new AdvancedCriteria(OperatorId.AND, list);
		
		assertEquals("returned query does not match expected one", 
				"SELECT COUNT(*) FROM tbTest WHERE BINARY Field_A LIKE '%SSS' and Field_B LIKE '%XXX%'", 
				manager.getCountQuery("tbTest", ac));
	}
	
	@Test
	public void getUpdateQueryTest() {
		assertEquals("returned query does not match expected one", 
						"UPDATE tbTest SET a = :1, b = :2 WHERE TestID = :TestID", 
						manager.getUpdateQuery("tbTest", "a = :1, b = :2", "TestID"));
	}
	
	@Test
	public void getDeleteByPrimaryKeyQueryTest() {
		assertEquals("returned query does not match expected one", 
						"DELETE FROM tbTest WHERE TestID = ?", 
						manager.getDeleteByPrimaryKeyQuery("tbTest", "TestID"));
	}
	
	@Test
	public void getDeleteByPrimaryKeysQueryTest() {
		assertEquals("returned query does not match expected one", 
						"DELETE FROM tbTest WHERE TestID IN (1,2,3)", 
						manager.getDeleteByPrimaryKeysQuery("tbTest", "TestID", new String[] { "1", "2", "3" }));
	}
	
	@Test
	public void getDeleteByParentKeysQueryTest() {
		assertEquals("returned query does not match expected one", 
						"DELETE FROM tbTest WHERE TestID IN (1,2,3)", 
						manager.getDeleteByParentKeysQuery("tbTest", "TestID", new String[] { "1", "2", "3" }));
	}
	
	@Test
	public void getDeleteByParentKeyQueryTest() {
		assertEquals("returned query does not match expected one", 
						"DELETE FROM tbTest WHERE TestID = ?", 
						manager.getDeleteByParentKeyQuery("tbTest", "TestID"));
	}
	
}
