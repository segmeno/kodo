package com.segmeno.kodo.entity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;

public class User extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public String name;
	
	public String passwordHash;
	
	public Date createdAt;
	
	@MappingRelation(masterColumnName="ID", joinedColumnName="UserID")
	public List<Address> addresses = new ArrayList<Address>();

	@MappingRelation(mappingTableName="tbUserRole", masterColumnName="UserID", joinedColumnName="RoleID")
	public List<Role> roles = new ArrayList<Role>();
	
	@MappingRelation(masterColumnName="ClearanceLevelID", joinedColumnName="ID")
	public Type clearanceLevel;
	
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
