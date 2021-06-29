package com.segmeno.kodo.database;

import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.Operator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WherePart {

	private static final Logger log = LogManager.getLogger(WherePart.class);

	private static final SimpleDateFormat DB_DATETIME_FORMAT = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");

	private final Set<Operator> ALLOWED_LIST_OPERATORS = new HashSet<Operator>() {
		private static final long serialVersionUID = 2809868650704689743L;
		{
			add(Operator.IN_SET);
			add(Operator.NOT_IN_SET);
			add(Operator.BETWEEN);
		}
	};

	protected final String sql;
	protected List<Object> params = new ArrayList<>();
	protected List<String> columnNames = new ArrayList<>();
	protected final String dbProduct;

	/**
	 *
	 * @param tableAlias - the alias of the table
	 * @param adCrit - the filter settings to be used
	 * @throws Exception
	 */
	public WherePart(final String tableAlias, final CriteriaGroup adCrit) throws Exception {
		this(null, tableAlias, adCrit);
	}

	/**
	 * @param dbProduct - the database vendor
	 * @param tableAlias - the alias of the table
	 * @param adCrit - the filter settings to be used
	 * @throws Exception
	 */
	public WherePart(final String dbProduct, final String tableAlias, final CriteriaGroup adCrit) throws Exception {
		this(dbProduct, tableAlias, null, adCrit);
	}

	/**
	 * @param tableAlias - the alias of the table
	 * @param columnNames - a list of all existing column names. If this parameter is set, sanity checks will be done while constructing the where part
	 * @param adCrit - the filter settings to be used
	 * @throws Exception
	 */
	public WherePart(final String tableAlias, final List<String> columnNames, final CriteriaGroup adCrit) throws Exception {
		this(null, tableAlias, columnNames, adCrit);
	}

	/**
	 * @param dbProduct - the database vendor
	 * @param tableAlias - the alias of the table
	 * @param columnNames - a list of all existing column names. If this parameter is set, sanity checks will be done while constructing the where part
	 * @param adCrit - the filter settings to be used
	 * @throws Exception
	 */
	public WherePart(final String dbProduct, String tableAlias, final List<String> columnNames, CriteriaGroup adCrit) throws Exception {
		this.dbProduct = dbProduct;
		if (columnNames != null) {
			this.columnNames = columnNames.stream().map(col -> col.toUpperCase()).collect(Collectors.toList());
		}

		tableAlias = tableAlias != null ? tableAlias + "." : "";

		if (adCrit == null) {
			adCrit = new CriteriaGroup();
		}

		final String tmp = addCriterias(tableAlias, adCrit);
		sql = tmp != null && tmp.length() > 2 ? tmp : "(1 = 1)";
	}

	private String addCriterias(final String tableAlias, final CriteriaGroup cg) throws Exception {
		final List<Criteria> crits = cg.getCriterias();
		final StringBuilder sb = new StringBuilder(1024);
		if (crits.size() > 0) {
			sb.append("(");

			for (final Criteria crit : crits) {
				if (crit == null) {
					continue;
				}

				if(crit.getCriteriaGroup() != null) {
					final String tmp = addCriterias(tableAlias, crit.getCriteriaGroup());
					if(tmp == null || tmp.length() < 3) {
						continue;
					}
					sb.append(tmp);
				} else {

					// first check if the column name is really existing
					if (!this.columnNames.isEmpty()) {
						if (crit.getFieldName() != null && !this.columnNames.contains(crit.getFieldName().toUpperCase())) {
							final String s = "Check your filter settings: Column with Name '" + crit.getFieldName() + "' used in criteria, but not existing in table " + tableAlias;
							log.error(s);
							throw new Exception(s);
						}
					}

					switch (crit.getOperator()) {
					case CONTAINS:
						sb.append(contains(tableAlias, crit));
						break;
					case ICONTAINS:
						sb.append(icontains(tableAlias, crit));
						break;
					case NOT_CONTAINS:
						sb.append(notContains(tableAlias, crit));
						break;
					case INOT_CONTAINS:
						sb.append(notContains(tableAlias, crit));
						break;
					case EQUALS:
						sb.append(equals(tableAlias, crit));
						break;
					case IEQUALS:
						sb.append(iequals(tableAlias, crit));
						break;
					case NOT_EQUAL:
						sb.append(not_equals(tableAlias, crit));
						break;
					case INOT_EQUAL:
						sb.append(inot_equals(tableAlias, crit));
						break;
					case GREATER_OR_EQUAL:
						sb.append(greaterOrEqual(tableAlias, crit));
						break;
					case GREATER_THAN:
						sb.append(greaterThan(tableAlias, crit));
						break;
					case LESS_OR_EQUAL:
						sb.append(lessOrEqual(tableAlias, crit));
						break;
					case LESS_THAN:
						sb.append(lessThan(tableAlias, crit));
						break;
					case STARTS_WITH:
						sb.append(startsWith(tableAlias, crit));
						break;
					case ISTARTS_WITH:
						sb.append(istartsWith(tableAlias, crit));
						break;
					case INOT_STARTS_WITH:
						sb.append(inotStartsWith(tableAlias, crit));
						break;
					case ENDS_WITH:
						sb.append(endsWith(tableAlias, crit));
						break;
					case IENDS_WITH:
						sb.append(iendsWith(tableAlias, crit));
						break;
					case INOT_ENDS_WITH:
						sb.append(inotEndsWith(tableAlias, crit));
						break;
					case IS_BLANK:
						sb.append(isBlank(tableAlias, crit));
						break;
					case NOT_BLANK:
						sb.append(notBlank(tableAlias, crit));
						break;
					case IS_NULL:
						sb.append(isNull(tableAlias, crit));
						break;
					case NOT_NULL:
						sb.append(notNull(tableAlias, crit));
						break;
					case IN_SET:
						sb.append(inSet(tableAlias, crit));
						break;
					case NOT_IN_SET:
						sb.append(notInSet(tableAlias, crit));
						break;
					case BETWEEN:
						sb.append(between(tableAlias,crit));
						break;
					default:
						throw new Exception("unsupported OperatorId " + crit.getOperator() + "! Please extend this class: " + this.getClass());
					}
				}
				sb.append(' ').append(cg.getOperator().getValue()).append(' ');
			}

			if (sb.length() > 1) {
				sb.setLength(sb.length() - (cg.getOperator().getValue().length() + 2));	// remove last ' OperatorId '
			}
			sb.append(")");
		}

		if(log.isDebugEnabled()) {
			log.debug("parsed " + cg + " to " + sb);
		}
		return sb.toString();
	}

	protected String contains(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add("%" + param + "%");
		return tableAlias + criteria.getFieldName() + " LIKE ?";
	}

	protected String icontains(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add("%" + param + "%");
		return "LOWER(" + tableAlias + criteria.getFieldName() + ") LIKE LOWER(?)";
	}

	protected String notContains(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add("%" + param + "%");
		return tableAlias + criteria.getFieldName() + " NOT LIKE ?";
	}

	protected String equals(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		params.add(getValue(criteria));
		return tableAlias + criteria.getFieldName() + " = ?";
	}

	protected String iequals(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		if (criteria.getStringValue() != null) {
			params.add(criteria.getStringValue());
			return "LOWER(" + tableAlias + criteria.getFieldName() + ") LIKE LOWER(?)";
		}
		else {
			params.add(getValue(criteria));
			return tableAlias + criteria.getFieldName() + " LIKE ?";
		}
	}

	protected String not_equals(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		params.add(getValue(criteria));
		return tableAlias + criteria.getFieldName() + " <> ?";
	}

	protected String inot_equals(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		if (criteria.getStringValue() != null) {
			params.add(criteria.getStringValue());
			return "LOWER(" + tableAlias + criteria.getFieldName() + ") NOT LIKE LOWER(?)";
		}
		else {
			params.add(getValue(criteria));
			return tableAlias + criteria.getFieldName() + " NOT LIKE ?";
		}
	}

	protected String greaterOrEqual(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		params.add(getValue(criteria));
		return tableAlias + criteria.getFieldName() + " >= ?";
	}

	protected String greaterThan(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		params.add(getValue(criteria));
		return tableAlias + criteria.getFieldName() + " > ?";
	}

	protected String lessOrEqual(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		params.add(getValue(criteria));
		return tableAlias + criteria.getFieldName() + " <= ?";
	}

	protected String lessThan(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		params.add(getValue(criteria));
		return tableAlias + criteria.getFieldName() + " < ?";
	}

	protected String startsWith(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add(param + "%");
		return tableAlias + criteria.getFieldName() + " LIKE ?";
	}

	protected String istartsWith(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add(param + "%");
		return "LOWER(" + tableAlias + criteria.getFieldName() + ") LIKE LOWER(?)";
	}

	protected String inotStartsWith(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add(param + "%");
		return "LOWER(" + tableAlias + criteria.getFieldName() + ") NOT LIKE LOWER(?)";
	}

	protected String endsWith(final String tableAlias, final Criteria criteria) {
		final Object param = getValueAsStr(criteria);
		params.add("%" + param);
		return tableAlias + criteria.getFieldName() + " LIKE ?";
	}

	protected String iendsWith(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add("%" + param);
		return "LOWER(" + tableAlias + criteria.getFieldName() + ") LIKE LOWER(?)";
	}

	protected String inotEndsWith(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Object param = getValueAsStr(criteria);
		params.add("%" + param);
		return "LOWER(" + tableAlias + criteria.getFieldName() + ") NOT LIKE LOWER(?)";
	}

	private String getValueAsStr(final Criteria criteria) {
	    final Object str = getValue(criteria);
	    if(str == null) {
	      return null;
	    }
	    if(str instanceof Date) {
          return DB_DATETIME_FORMAT.format((Date) str);
        }
	    if(str instanceof String) {
	      return (String) str;
	    }
	    return String.valueOf(str);
	}

    private Object getValue(final Criteria criteria) {
        return criteria.getStringValue() != null ? criteria.getStringValue() : (criteria.getNumberValue() != null ? criteria.getNumberValue() : criteria.getDateValue());
    }

	protected String isBlank(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		return "(" + tableAlias + criteria.getFieldName() + " = '' OR " + tableAlias + criteria.getFieldName() + " IS NULL)";
	}

	protected String notBlank(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		return "(" + tableAlias + criteria.getFieldName() + " != '' AND " + tableAlias + criteria.getFieldName() + " IS NOT NULL)";
	}

	protected String notNull(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		return tableAlias + criteria.getFieldName() + " IS NOT NULL";
	}

	protected String isNull(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		return tableAlias + criteria.getFieldName() + " IS NULL";
	}

	protected String inSet(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Class<?> type = determineListType(criteria.getListValues());
        params.addAll(criteria.getListValues());
        final String csv = criteria.getListValues().stream().map(val -> "?").collect(Collectors.joining(","));
		return tableAlias + criteria.getFieldName() + " IN (" + csv + ")";
	}

	protected String notInSet(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		final Class<?> type = determineListType(criteria.getListValues());
        params.addAll(criteria.getListValues());
        final String csv = criteria.getListValues().stream().map(val -> "?").collect(Collectors.joining(","));
		return tableAlias + criteria.getFieldName() + " NOT IN (" + csv + ")";
	}

	protected String between(final String tableAlias, final Criteria criteria) throws Exception {
		validateCriteria(criteria);
		if (criteria.getListValues().size() != 2) {
			throw new Exception("Expected exactly two list values to use the BETWEEN operator!");
		}
		final Class<?> type = determineListType(criteria.getListValues());
		params.add(criteria.getListValues().get(0));
        params.add(criteria.getListValues().get(1));
        return tableAlias + criteria.getFieldName() + " BETWEEN ? AND ?";
	}


	public boolean isEmpty() {
		return sql.length() == 0;
	}

	@Override
	public String toString() {
		return sql;
	}

	public List<Object> getValues() {
		return params;
	}

	/**
	 * checks if the combination of Operator and value type is correct
	 * @param criteria
	 * @throws Exception
	 */
	private void validateCriteria(final Criteria criteria) throws Exception {
		// check if a list value is set, but a unary operator is used
		if ((criteria.getListValues() != null && !criteria.getListValues().isEmpty()) && !ALLOWED_LIST_OPERATORS.contains(criteria.getOperator())) {
			final String msg = "the operator '" + criteria.getOperator() + "' does not support list values";
			log.error(msg);
			throw new Exception(msg);
		}
		// check if a list operator is used, but no list value is set
		if ((criteria.getListValues() == null || criteria.getListValues().isEmpty()) && ALLOWED_LIST_OPERATORS.contains(criteria.getOperator())) {
			final String msg = "the operator '" + criteria.getOperator() + "' supports list values only. Please provide them";
			log.error(msg);
			throw new Exception(msg);
		}
	}

	private Class determineListType(final List<?> list) {
		if (list != null && list.size() > 0) {
			final Object o = list.get(0);
			return o.getClass();
		}
		return null;
	}

}

