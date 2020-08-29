package com.segmeno.kodo.database;

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

public class DatabaseEntityTest {

	protected static final Logger LOG = LogManager.getLogger(DatabaseEntityTest.class);
	
	@BeforeClass
	public static void init() throws Exception {
		LOG.info("initializing DatabaseEntityTest");
	}

	@Test
	public void getSelectByPrimaryKeyQueryTest() {
	}
	
}
