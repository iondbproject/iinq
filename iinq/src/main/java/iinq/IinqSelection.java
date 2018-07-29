package iinq;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.metadata.IinqTable;
import unity.annotation.SourceField;

import javax.management.relation.RelationNotFoundException;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;

public class IinqSelection {
	protected ArrayList<String> whereValues; 			/* Value that is being added to another value value to update a value. */
	protected ArrayList<String> whereOperators; 		/* Whether values are being updated through addition or subtraction. */
	protected ArrayList<Integer> whereFieldTypes;
	protected ArrayList<Integer> whereFieldNums;
	protected int numConditions;

	public IinqSelection() {
		this.numConditions = 0;
		this.whereValues = new ArrayList<>();
		this.whereOperators = new ArrayList<>();
		this.whereFieldTypes = new ArrayList<>();
		this.whereFieldNums = new ArrayList<>();
	}

	public IinqSelection(ArrayList<String> conditionFields, IinqTable table) {
		this();

		if (conditionFields != null) {
			Iterator<String> it = conditionFields.iterator();

			while (it.hasNext()) {
				addCondition(it.next(), table);
			}
		}
/*		for (int i = 0; i < numConditions; i++) {
			int pos = -1, len = -1;
			*//* Set up value, operator, and condition for each WHERE clause *//*
			if (conditionFields.get(i).contains("!=")) {
				pos = conditionFields.get(i).indexOf("!=");
				len = 2;
				this.whereOperators.add("iinq_not_equal");
			} else if (conditionFields.get(i).contains("<>")) {
				pos = conditionFields.get(i).indexOf("<>");
				len = 2;
				this.whereOperators.add("iinq_not_equal");
			} else if (conditionFields.get(i).contains("<=")) {
				pos = conditionFields.get(i).indexOf("<=");
				len = 2;
				this.whereOperators.add("iinq_less_than_equal_to");
			} else if (conditionFields.get(i).contains(">=")) {
				pos = conditionFields.get(i).indexOf(">=");
				len = 2;
				this.whereOperators.add("iinq_greater_than_equal_to");
			} else if (conditionFields.get(i).contains("=")) {
				pos = conditionFields.get(i).indexOf("=");
				len = 1;
				this.whereOperators.add("iinq_equal");
			} else if (conditionFields.get(i).contains("<")) {
				pos = conditionFields.get(i).indexOf("<");
				len = 1;
				this.whereOperators.add("iinq_less_than");
			} else if (conditionFields.get(i).contains(">")) {
				pos = conditionFields.get(i).indexOf(">");
				len = 1;
				this.whereOperators.add("iinq_greater_than");
			}

			String fieldName = conditionFields.get(i).substring(0, pos).trim();
			whereValues.add(conditionFields.get(i).substring(pos + len).trim());
			SourceField field = table.getField(fieldName);
			whereFieldNums.add(field.getOrdinalPosition());
			whereFieldTypes.add(field.getDataType());

		}*/
	}

	public void addCondition(String condition, IinqTable table) {
		int pos = -1, len = -1;
		/* Set up value, operator, and condition for each WHERE clause */
		if (condition.contains("!=")) {
			pos = condition.indexOf("!=");
			len = 2;
			this.whereOperators.add("iinq_not_equal");
		} else if (condition.contains("<>")) {
			pos = condition.indexOf("<>");
			len = 2;
			this.whereOperators.add("iinq_not_equal");
		} else if (condition.contains("<=")) {
			pos = condition.indexOf("<=");
			len = 2;
			this.whereOperators.add("iinq_less_than_equal_to");
		} else if (condition.contains(">=")) {
			pos = condition.indexOf(">=");
			len = 2;
			this.whereOperators.add("iinq_greater_than_equal_to");
		} else if (condition.contains("=")) {
			pos = condition.indexOf("=");
			len = 1;
			this.whereOperators.add("iinq_equal");
		} else if (condition.contains("<")) {
			pos = condition.indexOf("<");
			len = 1;
			this.whereOperators.add("iinq_less_than");
		} else if (condition.contains(">")) {
			pos = condition.indexOf(">");
			len = 1;
			this.whereOperators.add("iinq_greater_than");
		}

		String fieldName = condition.substring(0, pos).trim();
		whereValues.add(condition.substring(pos + len).trim());
		SourceField field = table.getField(fieldName);
		whereFieldNums.add(field.getOrdinalPosition());
		whereFieldTypes.add(field.getDataType());
		numConditions++;
	}

	public ArrayList<String> getWhereValues() {
		return whereValues;
	}

	public ArrayList<String> getWhereOperators() {
		return whereOperators;
	}

	public ArrayList<Integer> getWhereFieldTypes() {
		return whereFieldTypes;
	}

	public ArrayList<Integer> getWhereFieldNums() {
		return whereFieldNums;
	}

	public int getNumConditions() {
		return numConditions;
	}

	public String generateIinqConditionList() {
		if (numConditions == 0) {
			return null;
		} else {
			StringBuilder conditionList = new StringBuilder();
			conditionList.append("IINQ_CONDITION_LIST(");
			for (int i = 0; i < numConditions; i++) {
				conditionList.append("IINQ_CONDITION(");
				conditionList.append(whereFieldNums.get(i));
				conditionList.append(", ");
				conditionList.append(whereOperators.get(i));
				conditionList.append(", ");
				if (whereFieldTypes.get(i) == Types.INTEGER) {
					conditionList.append(String.format("IONIZE(%s, int)", whereValues.get(i)));
				} else {
					conditionList.append(whereValues.get(i));
				}
				conditionList.append("), ");
			}
			conditionList.setLength(conditionList.length()-2);
			conditionList.append(")");

			return conditionList.toString();
		}
	}
}
