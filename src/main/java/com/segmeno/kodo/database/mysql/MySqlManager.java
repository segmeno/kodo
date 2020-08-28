package com.segmeno.kodo.database.mysql;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.segmeno.kodo.database.DataAccessManager;
import com.segmeno.kodo.transport.AdvancedCriteria;

@Component
public class MySqlManager extends DataAccessManager {

	protected final static Logger log = Logger.getLogger(MySqlManager.class);
	
	public MySqlManager(DataSource dataSource) {
		super(dataSource);
	}
	
	public MySqlManager(JdbcTemplate jdbcTemplate) {
		super(jdbcTemplate);
	}

	@Override
	protected String getSelectByPrimaryKeyQuery(String tableName, String primaryKeyColumnName) {
		return "SELECT * FROM " + tableName + " WHERE " + primaryKeyColumnName + " = ?";
	}

	@Override
	protected String getSelectByCriteriaQuery(String tableName, AdvancedCriteria advancedCriteria) throws Exception {
		final MySqlWherePart where = new MySqlWherePart(advancedCriteria);
		if (where.isEmpty()) {
			return "SELECT * FROM " + tableName;
		}
		return "SELECT * FROM " + tableName + " WHERE " + where.toString();
	}

	@Override
	protected String getCountQuery(String tableName, AdvancedCriteria advancedCriteria) throws Exception {
		final MySqlWherePart where = new MySqlWherePart(advancedCriteria);
		if (where.isEmpty()) {
			return "SELECT COUNT(*) FROM " + tableName;
		}
		return "SELECT COUNT(*) FROM " + tableName + " WHERE " + where.toString();
	}

	@Override
	protected String getUpdateQuery(String tableName, String params, String primaryKeyColumn) {
		return "UPDATE " + tableName + " SET " + params + " WHERE " + primaryKeyColumn + " = :" + primaryKeyColumn;
	}

	@Override
	protected String getDeleteByPrimaryKeyQuery(String tableName, String primaryKeyColumnName) {
		return "DELETE FROM " + tableName + " WHERE " + primaryKeyColumnName + " = ?";
	}

	@Override
	protected String getDeleteByPrimaryKeysQuery(String tableName, String primaryKeyColumnName, String[] primaryKeyIds) {
		return "DELETE FROM " + tableName + " WHERE " + primaryKeyColumnName + " IN (" + toCsv(primaryKeyIds) + ")";
	}

	@Override
	protected String getDeleteByParentKeyQuery(String tableName, String parentKeyColumnName) {
		return "DELETE FROM " + tableName + " WHERE " + parentKeyColumnName + " = ?";
	}

	@Override
	protected String getDeleteByParentKeysQuery(String tableName, String parentKeyColumnName, String[] parentKeyIds) {
		return "DELETE FROM " + tableName + " WHERE " + parentKeyColumnName + " IN (" + toCsv(parentKeyIds) + ")";
	}
	

}