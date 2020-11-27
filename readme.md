[![Build Status](https://travis-ci.com/segmeno/kodo.svg?branch=master)](https://travis-ci.com/segmeno/kodo)

This Project is a lightweight database access layer for quick data inserts, updates, deletes or reads.

# Usage

## Setup of Database Classes

Let your container object (the table representation) extend from the DatabaseEntity class and use annotations to further describe your fields and relations.

All Fields of the class are automatically considered to be columns. If a column does not exist in the database table, the @DbIgnore annotation can be used. If the name of the column deviates from the variable name, the @column(columnName="<name>") annotation can be used.
The primary key column must be marked with the @PrimaryKey annotation. We strongly suggest using a Long datatype as PK. If you need other keys to your data, best practise is to create unique indexes and just keep the primary key a number (this is much more efficient for relations).

Entities which are coming from another table can also be added as variables. These classes must also inherit the DatabaseEntity class and required to be marked with the @MappingRelation annotation. If the relation is many to many, the mappingTableName must be set. If the relation is one to many, only the masterColumnName and joinedColumnName values must be set.

The example below shows all possible configurations: the field id is marked as primary key. The field pwHash is named differently in the database table. the field notExistingInDb is excluded via the @DbIgnore annotation. The List of TestAddresses is representing data from another table and therefore annotated with the @MappingRelation annotation. In this example a user can have multiple addresses. 
Roles and Users are connected via a many to many table, called tbUserRole.

```
public class TestUser extends DatabaseEntity {

	@PrimaryKey
	public Long id;
	
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

### Mapping sub entities, the need for an PK != null and PK < 0

As shown in the example above, your main entity TestUser aggregates multiple SubEntities TestType, TestAddress, TestRole. The kodo framework will try to fetch one or multiple TestUser objects and all their sub entities in one SQL statement. There are many cases this is much faster than executing multiple statements to fill the object. If you have a case, where this should prove to create to much overhead, just use @DbIgnore and fetch some of the sub entities yourself.

The way fetching everything in one statement works, kodo alwas needs a PK for each object and sub object. Sometimes this seems a little inpractical, like the following DB view shows. Here we have an attribute definition customizable by the user, so she can define additional values with any object type we have in our application. In the GUI we want to display each additional value defined for the object type, regardless whether the user has already created one fpr the particular object: 

```
CREATE OR ALTER VIEW vwCamContractCustomAttributes AS
	SELECT ca.CustomAttributeID, cad.CustomAttributeDefinitionID, ca.ObjectID, ca.Value
		FROM tbCustomAttributeDefinition cad 
		JOIN tbCustomAttribute ca ON ca.CustomAttributeDefinitionID = cad.CustomAttributeDefinitionID
		WHERE cad.Type = 'Contract'
	UNION
	SELECT -1 * row_number() over(ORDER BY cv.ContractVersionID) AS CustomAttributeID, cad.CustomAttributeDefinitionID, c.ContractID AS ObjectID, NULL AS Value
		FROM tbCustomAttributeDefinition cad
		JOIN tbContract c ON c.ContractID NOT IN (SELECT ca.ObjectID FROM tbCustomAttribute ca WHERE ca.CustomAttributeDefinitionID = cad.CustomAttributeDefinitionID)
		WHERE cad.Type = 'Contract'
GO
```

Here you see, for the values that do not exist in tbCustomAttribute we create a virtual CustomAttributeID < 0, because kodo expects each existing Objekt to have an value for its primary key != null and > 0. Otherwise an call to update the entity will just create a new one.

### @CustomSql

If the database entity is not representing a table or if it is required to inject own SQL, the @CustomSql annotation can be used. This can be annotated on class level.
If used, the getTableName() method just needs to return null. Child entities which are of type List<? extends DatabaseEntity> do not need any @MappingRelation annotation. Instead, the custom sql query needs to be built in a way that all child entities are queried correctly.

```
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

