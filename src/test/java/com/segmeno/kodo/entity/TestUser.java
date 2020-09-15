package com.segmeno.kodo.entity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.segmeno.kodo.annotation.Column;
import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;

public class TestUser extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public String name;
	
	@Column(columnName="passwordHash")
	public String pwHash;
	
	public Date createdAt;
	
	@MappingRelation(masterColumnName="ID", joinedColumnName="UserID")
	public List<TestAddress> addresses = new ArrayList<TestAddress>();

	@MappingRelation(mappingTableName="tbUserRole", masterColumnName="UserID", joinedColumnName="RoleID")
	public List<TestRole> roles = new ArrayList<TestRole>();
	
	@MappingRelation(masterColumnName="ClearanceLevelID", joinedColumnName="ID")
	public TestType clearanceLevel;
	
	@DbIgnore
	public String notExistingInDb;

	@Override
	public String getTableName() {
		return "tbUser";
	}

	@Override
	public String toString() {
		try {
			return toMap().toString();
		} catch (Exception e) {
			return null;
		}
	}
	
}
