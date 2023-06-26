package com.segmeno.kodo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.ANNOTATION_TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MappingRelation {

	/**
	 * the name of the <b>many-to-many</b> table. If the relation is <b>one-to-many</b>, this field has to be left blank
	 * <br><br>
	 * i.e.: tbUser -> tbUserRole -> tbRole   <--- on User can have many roles and a role can be assigned to many users
	 * <br>	@MappingRelation(mappingTableName="tbUserRole", masterColumnName="UserID", joinedColumnName="RoleID")
	 * <br><br>
	 * i.e.: tbUser -> tbUserAddress   <--- a User can have multiple addresses (addresses are not reused for other users
	 * <br>	@MappingRelation(masterColumnName="ID", joinedColumnName="UserID")
	 * <br><br>
	 * i.e.: tbUser -> tbType   <--- a User has a "Clearance Level Type" but the Type can be possessed by many Users
	 * <br>	@MappingRelation(masterColumnName="ClearanceLevelID", joinedColumnName="ID") 
	 * 
	 * @return
	 */
	String mappingTableName() default "";
	
	/**
	 * the name of the main table column in the many-to-many table.
	 * If the relation is one-to-many, this refers to the primary key field of the main table
	 * @return
	 */
	String masterColumnName() default "";
	
	/**
	 * the name of the joined table column in the many-to-many table
	 * If the relation is one-to-many, this refers to the primary key field of the joined table
	 * @return
	 */
	String joinedColumnName() default "";
}
