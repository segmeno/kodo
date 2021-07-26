package com.segmeno.kodo.database;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.Operator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;

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
		assertTrue(w.toString().equals("(testtable.ID IN (?,?,?))"));
        assertTrue(w.getValues().size() == 3);
		assertTrue(w.getValues().containsAll(ids));
	}

	@Test
	public void notInSetTest_string() throws Exception {
		final List<String> names = new ArrayList<>();
		names.add("Tim");
		names.add("Tom");
		names.add("Bill");
		final CriteriaGroup cg = new CriteriaGroup(Operator.AND);
		cg.add(new Criteria("Name", Operator.NOT_IN_SET, names));

		final WherePart w = new WherePart("testtable", cg);
		assertTrue(w.toString().equals("(testtable.Name NOT IN (?,?,?))"));
        assertTrue(w.getValues().size() == 3);
        assertTrue(w.getValues().containsAll(names));
	}

    @Test
    public void betweenTest_date() throws Exception {
        final List<Date> dates = new ArrayList<>();
        dates.add(new Date(0));
        dates.add(new Date());
        final CriteriaGroup cg = new CriteriaGroup(Operator.AND);
        cg.add(new Criteria("Date", Operator.BETWEEN, dates));

        final WherePart w = new WherePart("testtable", cg);
        assertTrue(w.toString().equals("(testtable.Date BETWEEN ? AND ?)"));
        assertTrue(w.getValues().size() == 2);
        assertTrue(w.getValues().containsAll(dates));
    }

    @Test
    public void complexHierachyTest() throws Exception {
        final Date dateValue = new Date();
        final Integer intValue = Integer.valueOf(666);
        final String strValue = "foobar";

        final CriteriaGroup cgroot = new CriteriaGroup(Operator.AND);
        cgroot.add(new Criteria("Date", Operator.EQUALS, dateValue));
        final CriteriaGroup cgchild = new CriteriaGroup(Operator.OR);
        cgchild.add(new Criteria("Int", Operator.EQUALS, intValue));
        cgchild.add(new Criteria("Str", Operator.EQUALS, strValue));
        cgroot.add(new Criteria(cgchild));

        final WherePart w = new WherePart("testtable", cgroot);
        assertTrue(w.toString().equals("(testtable.Date = ? and (testtable.Int = ? or testtable.Str = ?))"));
        assertTrue(w.getValues().size() == 3);
        assertTrue(w.getValues().get(0).equals(dateValue));
        assertTrue(w.getValues().get(1).equals(intValue));
        assertTrue(w.getValues().get(2).equals(strValue));
    }

}
