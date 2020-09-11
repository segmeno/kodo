package com.segmeno.kodo.entity;

import java.util.Date;

import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;

public class Address extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public Integer userId;
	
	public String street;
	
	public String postalCode;
	
	public Date createdAt;
	
	@Override
	public String getTableName() {
		return "tbAddress";
	}

}
