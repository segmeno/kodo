package com.segmeno.kodo.database;

import java.util.List;

import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingTable;
import com.segmeno.kodo.annotation.PrimaryKey;

public class TestUser extends DatabaseEntity {

	@PrimaryKey
	private Integer testUserId;
	
	private String testUserName;
	
	@MappingTable("tbUserRole")
	private List<TestRole> roles;
	
	@DbIgnore
	private String notExistingInDb;

	@Override
	public void fillChildObjects(DataAccessManager manager) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getTableName() {
		return "tbTestUser";
	}

}
