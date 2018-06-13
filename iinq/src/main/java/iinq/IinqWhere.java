package iinq;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.metadata.IinqTable;

import javax.management.relation.RelationNotFoundException;
import java.awt.*;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;

import static iinq.IinqExecute.ion_get_value_size;
import static iinq.functions.SchemaKeyword.*;

public class IinqWhere {
	private String[] where_field_names; 		/* Field value that is being used to update a value. */
	private String[] where_values; 			/* Value that is being added to another value value to update a value. */
	private String[] where_operators; 		/* Whether values are being updated through addition or subtraction. */
	private String[] iinq_field_types;
	private String[] where_field_types;
	private Integer[] where_field_nums;
	private int num_conditions;
	private String[] field_sizes;

	public IinqWhere(int num_conditions) {
		this.num_conditions		= num_conditions;
		this.where_field_names	= new String[num_conditions];
		this.where_values		= new String[num_conditions];
		this.where_operators	= new String[num_conditions];
		this.where_field_types 	= new String[num_conditions];
		this.where_field_nums	= new Integer[num_conditions];
	}

	public String[] getWhere_field_names() {
		return where_field_names;
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

	public String[] getWhere_field_types() {
		return where_field_types;
	}

	public Integer[] getWhere_field_nums() {
		return where_field_nums;
	}

	public int getNum_conditions() {
		return num_conditions;
	}

	public String[] getField_sizes() {
		return where_field_types;
	}

	public void generateWhere(String[] conditionFields, IinqTable table) throws InvalidArgumentException, SQLFeatureNotSupportedException, RelationNotFoundException, IOException {
		if (conditionFields.length != this.num_conditions)
			throw new InvalidArgumentException(new String[]{"Array must be the same length as specified in the constructor."});
		int num_fields = Integer.parseInt(table.getSchemaValue(NUMBER_OF_FIELDS));
		this.iinq_field_types	= new String[num_fields*num_conditions];
		this.field_sizes		= new String[num_fields*num_conditions];
		for (int i = 0; i < num_conditions; i++) {
			int pos = -1, len = -1;
			/* Set up value, operator, and condition for each WHERE clause */
			if (conditionFields[i].contains("!=")) {
				pos = conditionFields[i].indexOf("!=");
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

			where_field_names[i] = conditionFields[i].substring(0, pos).trim();
			where_values[i] = conditionFields[i].substring(pos + len).trim();

			for (int j = 0; j < num_fields; j++) {
				String field_type = table.getSchemaValue(FIELD_TYPE, j+1);
				field_sizes[j] = table.getIonFieldSize(j+1);

				if (field_type.contains("CHAR")) {
					iinq_field_types[i*num_conditions + j] = "iinq_char";
				} else {
					iinq_field_types[i*num_conditions + j] = "iinq_int";
				}

				if (where_field_names[i].equalsIgnoreCase(table.getSchemaValue(FIELD_NAME, j+1))) {
					where_field_nums[i] = j + 1;
					where_field_types[i] = field_type;
				}
			}
		}
	}
}
