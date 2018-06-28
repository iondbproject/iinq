package iinq;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.metadata.IinqTable;
import unity.annotation.SourceField;

import javax.management.relation.RelationNotFoundException;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

public class IinqWhere {
	private String[] where_values; 			/* Value that is being added to another value value to update a value. */
	private String[] where_operators; 		/* Whether values are being updated through addition or subtraction. */
	private String[] iinq_field_types;
	private int[] where_field_types;
	private Integer[] where_field_nums;
	private int num_conditions;

	public IinqWhere(int num_conditions) {
		this.num_conditions		= num_conditions;
		this.where_values		= new String[num_conditions];
		this.where_operators	= new String[num_conditions];
		this.where_field_types 	= new int[num_conditions];
		this.where_field_nums	= new Integer[num_conditions];
	}

	public String[] getWhere_values() {
		return where_values;
	}

	public String[] getWhere_operators() {
		return where_operators;
	}

	public String[] getIinq_field_types() {
		return iinq_field_types;
	}

	public int[] getWhere_field_types() {
		return where_field_types;
	}

	public Integer[] getWhere_field_nums() {
		return where_field_nums;
	}

	public int getNum_conditions() {
		return num_conditions;
	}

	public int[] getField_sizes() {
		return where_field_types;
	}

	public String generateIinqConditionList() {
		if (num_conditions == 0) {
			return null;
		} else {
			StringBuilder conditionList = new StringBuilder();
			conditionList.append("IINQ_CONDITION_LIST(");
			for (int i = 0; i < num_conditions; i++) {
				conditionList.append("IINQ_CONDITION(");
				conditionList.append(where_field_nums[i]);
				conditionList.append(", ");
				conditionList.append(where_operators[i]);
				conditionList.append(", ");
				if (where_field_types[i] == Types.INTEGER) {
					conditionList.append(String.format("IONIZE(%s, int)", where_values[i]));
				} else {
					conditionList.append(where_values[i]);
				}
				conditionList.append("), ");
			}
			conditionList.setLength(conditionList.length()-2);
			conditionList.append(")");

			return conditionList.toString();
		}
	}

	public void generateWhere(String[] conditionFields, IinqTable table) throws InvalidArgumentException, SQLFeatureNotSupportedException, RelationNotFoundException, IOException {
		if (conditionFields.length != this.num_conditions)
			throw new InvalidArgumentException(new String[]{"Array must be the same length as specified in the constructor."});
		int num_fields = table.getNumFields();
		this.iinq_field_types	= new String[num_fields*num_conditions];
		for (int i = 0; i < num_conditions; i++) {
			int pos = -1, len = -1;
			/* Set up value, operator, and condition for each WHERE clause */
			if (conditionFields[i].contains("!=")) {
				pos = conditionFields[i].indexOf("!=");
				len = 2;
				this.where_operators[i] = "iinq_not_equal";
			} else if (conditionFields[i].contains("<>")) {
				pos = conditionFields[i].indexOf("<>");
				len = 2;
				this.where_operators[i] = "iinq_not_equal";
			} else if (conditionFields[i].contains("<=")) {
				pos = conditionFields[i].indexOf("<=");
				len = 2;
				this.where_operators[i] = "iinq_less_than_equal_to";
			} else if (conditionFields[i].contains(">=")) {
				pos = conditionFields[i].indexOf(">=");
				len = 2;
				this.where_operators[i] = "iinq_greater_than_equal_to";
			} else if (conditionFields[i].contains("=")) {
				pos = conditionFields[i].indexOf("=");
				len = 1;
				this.where_operators[i] = "iinq_equal";
			} else if (conditionFields[i].contains("<")) {
				pos = conditionFields[i].indexOf("<");
				len = 1;
				this.where_operators[i] = "iinq_less_than";
			} else if (conditionFields[i].contains(">")) {
				pos = conditionFields[i].indexOf(">");
				len = 1;
				this.where_operators[i] = "iinq_greater_than";
			}

			String fieldName = conditionFields[i].substring(0, pos).trim();
			where_values[i] = conditionFields[i].substring(pos + len).trim();
			SourceField field = table.getField(fieldName);
			where_field_nums[i] = field.getOrdinalPosition();
			where_field_types[i] = field.getDataType();
		}
	}
}
