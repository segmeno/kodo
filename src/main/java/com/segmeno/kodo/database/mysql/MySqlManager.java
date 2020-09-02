package com.segmeno.kodo.database.mysql;

import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.segmeno.kodo.database.DataAccessManager;
import com.segmeno.kodo.database.DatabaseEntity;

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
	protected String getSelectQuery(DatabaseEntity mainEntity, List<DatabaseEntity> childEntitiesToJoin) throws Exception {
		String select = "SELECT " + getColumnsCsvWithAlias(mainEntity.getTableName(), mainEntity.getColumnNames(true));
		String from = " FROM " + mainEntity.getTableName();
		String join = "";
		String where = "";
		
		for (DatabaseEntity child : childEntitiesToJoin) {
			select += ", " + getColumnsCsvWithAlias(child.getTableName(), child.getColumnNames(true));
			if (child.mappingTable != null) {
				final String masterCol = child.mappingTable.masterColumnName().isEmpty() ? mainEntity.getPrimaryKeyColumn() : child.mappingTable.masterColumnName();
				final String joinedCol = child.mappingTable.joinedColumnName().isEmpty() ? child.getPrimaryKeyColumn() : child.mappingTable.joinedColumnName();
				join += " LEFT JOIN " + child.mappingTable.value() + " ON " + child.mappingTable.value() + "." + masterCol + " = " + mainEntity.getTableName() + "." + mainEntity.getPrimaryKeyColumn();
				join += " LEFT JOIN " + child.getTableName() + " ON " + child.getTableName() + "." + child.getPrimaryKeyColumn() + " = " + child.mappingTable.value() + "." + joinedCol;
			}
			else {
				join += " LEFT JOIN " + child.getTableName() + " ON " + child.getTableName() + "." + child.getPrimaryKeyColumn() + " = " + mainEntity.getTableName() + "." + child.getPrimaryKeyColumn();
			}
		}
		
		if (mainEntity.advancedCriteria != null) {
			final MySqlWherePart mainWherePart = new MySqlWherePart(mainEntity.getTableName(), mainEntity.advancedCriteria);
			if (!mainWherePart.isEmpty()) {
				where = " WHERE " + mainWherePart.toString();
			}
		}
		for (DatabaseEntity child : childEntitiesToJoin) {
			if (child.advancedCriteria != null) {
				final MySqlWherePart childWherePart = new MySqlWherePart(child.getTableName(), child.advancedCriteria);
				where += where.isEmpty() ? " WHERE " + childWherePart.toString() : " AND " + childWherePart.toString();
			}
		}
		
		return select + from + join + where;
	}

	@Override
	protected String getCountQuery(String sql) throws Exception {
		return "SELECT COUNT(*) FROM (" + sql + ")";
	}

	@Override
	protected String getUpdateQuery(String tableName, String params, String primaryKeyColumn) throws Exception {
		return "UPDATE " + tableName + " SET " + params + " WHERE " + primaryKeyColumn + " = :" + primaryKeyColumn;
	}

	@Override
	protected String getDeleteQuery(DatabaseEntity mainEntity) throws Exception {
		return "DELETE FROM " + mainEntity.getTableName() + " WHERE " + mainEntity.getPrimaryKeyColumn() + " = ?";
	}

}