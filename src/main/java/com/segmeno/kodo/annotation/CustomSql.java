package com.segmeno.kodo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;

@Target({ElementType.ANNOTATION_TYPE, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface CustomSql {

	/**
	 * sql query to be used as select for the annotated class
	 * @return
	 */
	String selectQuery() default "";

	/**
	 * the columns that are allowed to be used in WHERE part of the generated queries
	 * @return
	 */
	String[] knownColumnNames();
}
