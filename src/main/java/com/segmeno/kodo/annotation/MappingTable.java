package com.segmeno.kodo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MappingTable {

	/**
	 * the name of the many-to-many table
	 * @return
	 */
	String value() default "";
	
	/**
	 * the name of the master table column in the many-to-many table.
	 * Only required if the column name deviates from the name in the master table
	 * @return
	 */
	String masterColumnName() default "";
	
	/**
	 * the name of the joined table column in the many-to-many table
	 * Only required if the column name deviates from the name in the joined table
	 * @return
	 */
	String joinedColumnName() default "";
}
