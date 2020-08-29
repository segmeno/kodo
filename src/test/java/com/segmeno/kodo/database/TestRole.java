package com.segmeno.kodo.database;

import com.segmeno.kodo.annotation.PrimaryKey;

public class TestRole extends DatabaseEntity {

	@PrimaryKey
	private Integer testRoleId;
	
	private String testRoleName;
	
	@Override
	public void fillChildObjects(DataAccessManager manager) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public String getTableName() {
		return "tbTestRole";
	}

}
