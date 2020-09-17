[![Build Status](https://travis-ci.com/segmeno/kodo.svg?branch=master)](https://travis-ci.com/segmeno/kodo)

This Project is a lightweight database access layer for quick data inserts, updates, deletes or reads.

# Usage

## Setup of Database Classes

let your container object (the table representation) extend from the DatabaseEntity class and use annotations to further describe your fields and relations.

All Fields of the class are automatically considered to be columns. If a column does not exist in the database table, the @DbIgnore annotation can be used. If the name of the column deviates from the variable name, the @column(columnName="<name>") annotation can be used.
The primary key column must be marked with the @PrimaryKey annotation.

Entities which are coming from another table can also be added as variables. These classes must also inherit the DatabaseEntity class and required to be marked with the @MappingRelation annotation. If the relation is many to many, the mappingTableName must be set. If the relation is one to many, only the masterColumnName and joinedColumnName values must be set.

The example below shows all possible configurations: the field id is marked as primary key. The field pwHash is named differently in the database table. the field notExistingInDb is excluded via the @DbIgnore annotation. The List of TestAddresses is representing data from another table and therefore annotated with the @MappingRelation annotation. In this example a user can have multiple addresses. 
Roles and Users are connected via a many to many table, called tbUserRole.

```
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
}
```

## Setup of the DataAccessManager Class

the Data Access Manager can be instantiated by passing in a JdbcTemplate or a DataSource object.

```
DataSource ds = ...
manager = new DataAccessManager(ds);
```
it is as simple as that: Now the Data Access Manager can be used to add, get, update or delete entities.

## Using the Criteria Class

most methods of the Data Access Manager allow to pass in a Criteria. This is a Filter which will be applied when fetching the data. Alternatively, a CriteriaGroup can be used to combine single criterias.

The following CriteriaGroup makes sure that the results are either for Elmstreet or Testplace:

```
CriteriaGroup cg = new CriteriaGroup(Operator.OR)
					.add(new Criteria("Street", Operator.EQUALS, "Elmstreet"))
					.add(new Criteria("Street", Operator.EQUALS, "Testplace"));
```

## getting elements

the getElem methods can be used to retrieve the desired elements. CriteriaGroups and Criterias are always applied to the main Entity only. So if the TestUser.class is being passed in, all Criteria fields must be columns of the user table. The optional parameter fetchDepth controls how deep the entities should be filled. If the TestUser.class is used again, a fetchDepth of 0 will only fetch data from the user table. To also retrieve roles for the users, the fetchDepth must be set to 1. To fetch all data, this parameter does not need to be filled or must be set to -1.

## deleting elements

when deleting elements, all data from 1:n tables will be deleted too. If one user has multiple addresses and the user should be deleted, automatically all its addresses will be deleted as well. If there is a m:n relationship to other tables, only the entries from this mapping table will be deleted. That means for a user with roles, all the roles will be preserved and only unassigned from the user first by removing the mapping table entries.

## updating elements

updating requires the primary key value to be set. If not, the element will be added instead. Update also affects all child elements. Child elements which are existing only in the database (but are not present inside the main entity) will not be deleted by the update method.

## adding elements

this will add all required child elements first and all depending child elements after inserting the main entity.
