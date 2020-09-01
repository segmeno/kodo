package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabaseEntityTest {

	protected static final Logger LOG = LogManager.getLogger(DatabaseEntityTest.class);
	
	@BeforeClass
	public static void init() throws Exception {
		LOG.info("initializing DatabaseEntityTest");
	}

	@Test
	public void mappingTableAnnotationTest() throws IllegalArgumentException, IllegalAccessException, ClassNotFoundException {
		
		final User user = new User();
		for (Field f : user.fields) {
			System.out.println(f.getName() + " = " + f.get(user));
			if (Collection.class.isAssignableFrom(f.getType())) {
				System.out.println(f.getName() + " is a Collection");
				ParameterizedType genType = (ParameterizedType) f.getGenericType();
				
				Type genericType = genType.getActualTypeArguments()[0];
				System.out.println("Collection holds elements of type: " + genericType.getTypeName());
				Class<?> genericClass = Class.forName(genericType.getTypeName());
				System.out.println(genericType.getTypeName() + " is subtype of DatabaseEntity? " + DatabaseEntity.class.isAssignableFrom(genericClass));
			}
		}
	}
	
}
