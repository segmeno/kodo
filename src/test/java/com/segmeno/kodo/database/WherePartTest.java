package com.segmeno.kodo.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.Operator;

public class WherePartTest {
	
	@Test
	public void inSetTest_integer() throws Exception {
		final List<Integer> ids = new ArrayList<>();
		ids.add(1);
		ids.add(2);
		ids.add(3);
		final CriteriaGroup cg = new CriteriaGroup(Operator.AND);
		cg.add(new Criteria("ID", Operator.IN_SET, ids));
		
		final WherePart w = new WherePart("testtable", cg);
		assertTrue(w.toString().equals("(testtable.ID IN (1,2,3))"));
	}
	
	@Test
	public void inSetTest_string() throws Exception {
		final List<String> names = new ArrayList<>();
		names.add("Tim");
		names.add("Tom");
		names.add("Bill");
		final CriteriaGroup cg = new CriteriaGroup(Operator.AND);
		cg.add(new Criteria("ID", Operator.IN_SET, names));
		
		final WherePart w = new WherePart("testtable", cg);
		assertTrue(w.toString().equals("(testtable.ID IN ('Tim','Tom','Bill'))"));
	}

}
