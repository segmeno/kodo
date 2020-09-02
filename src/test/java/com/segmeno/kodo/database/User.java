package com.segmeno.kodo.database;

import java.util.ArrayList;
import java.util.List;

import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingTable;
import com.segmeno.kodo.annotation.PrimaryKey;

public class User extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public String userFirstName;
	
	public String userLastName;
	
	public String userSign;
	
	public String userPassword;
	
	public String createdBy;
	
	public String modifiedBy;
	
	public int flag;
	
	@MappingTable(value="tbUserRole", masterColumnName="UserID", joinedColumnName="RoleID")
	public List<Role> roles = new ArrayList<Role>();
	
	@DbIgnore
	public String notExistingInDb;

	@Override
	public String getTableName() {
		return "tbUser";
	}

}
