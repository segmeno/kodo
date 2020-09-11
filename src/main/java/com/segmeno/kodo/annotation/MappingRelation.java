package com.segmeno.kodo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MappingRelation {

	/**
	 * the name of the many-to-many table. If the relation is one-to-many, this field can be left blank
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
