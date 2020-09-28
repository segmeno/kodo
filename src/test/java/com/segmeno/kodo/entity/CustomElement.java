package com.segmeno.kodo.entity;

import java.util.ArrayList;
import java.util.List;

import com.segmeno.kodo.annotation.CustomSql;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;

@CustomSql(selectQuery="SELECT u.id AS CustomElementID, u.Name AS customName, 3 AS customAmount, "
		+ "a.id AS \"customaddress.ID\", a.userId AS \"customaddress.UserID\", "
		+ "a.street AS \"customaddress.street\", a.postalCode AS \"customaddress.postalCode\" "
		+ "FROM tbUser u LEFT JOIN tbAddress a ON a.UserID = u.ID")
public class CustomElement extends DatabaseEntity {
	
	@PrimaryKey
	public Integer customElementId;
	
	public String customName;
	
	public List<TestAddress> customaddress = new ArrayList<>();
	
	public Integer customAmount;
	
	@Override
	public String getTableName() {
		return null;
	}

}
