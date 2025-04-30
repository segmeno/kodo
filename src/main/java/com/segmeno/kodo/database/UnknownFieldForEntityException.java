package com.segmeno.kodo.database;

/**
 * @author chu
 */
public class UnknownFieldForEntityException extends Exception {
	private static final long serialVersionUID = 4081669831119954989L;

	public UnknownFieldForEntityException(String fieldName) {
		super("Unknown field: " + fieldName);
	}
}
