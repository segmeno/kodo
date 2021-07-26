package com.segmeno.kodo.transport;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

public class TransportTest {


	@Test
	public void test_toString() throws Exception {
		final List<String> names = new ArrayList<>();
		names.add("Tim");
		names.add("Tom");
		names.add("Bill");
		final CriteriaGroup cg = new CriteriaGroup(Operator.AND);
		cg.add(new Criteria("ID", Operator.IN_SET, names));
		cg.add(new Criteria("ID", Operator.EQUALS, 1));

		System.out.println(cg.toString());
	}

}
