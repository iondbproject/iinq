package iinq.functions;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.IinqInsert;
import iinq.IinqInsertFields;
import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import iinq.tableInfo;
import unity.jdbc.UnityPreparedStatement;
import unity.query.GQFieldRef;
import unity.query.GlobalUpdate;
import unity.query.LQExprNode;
import unity.query.LQInsertNode;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import static iinq.functions.SchemaKeyword.*;

public class PreparedInsertFunction extends IinqFunction {
	public tableInfo table_info;
	private static ArrayList<Integer> tablesWritten = new ArrayList<>();
	private static boolean setParamsWritten = false;
	private GlobalUpdate globalUpdate;
	private IinqDatabase iinqDatabase;
	private boolean duplicate;
	private boolean preparedStatement;
	private IinqInsert insertParameters;

	public PreparedInsertFunction(GlobalUpdate globalUpdate, IinqDatabase iinqDatabase, ExecuteFunction executeFunction) throws SQLException, InvalidArgumentException {
		this.iinqDatabase = iinqDatabase;
		this.globalUpdate = globalUpdate;
		processInsert(executeFunction);
	}

	public void processInsert(ExecuteFunction executeFunction) throws SQLException, InvalidArgumentException {
		LQInsertNode insertNode = (LQInsertNode) globalUpdate.getPlan().getLogicalQueryTree().getRoot();
		IinqTable table = iinqDatabase.getIinqTable(insertNode.getSourceTable().getTable().getTableName());

		setName("insert_"+ table.getTableName());
		generatePreparedFunction(executeFunction);
		// See if we need to make a completely new function
		if (!tablesWritten.contains(table.getTableId())) {
			duplicate = false;
			tablesWritten.add(table.getTableId());
		} else {
			setHeader(null);
			setDefinition(null);
			duplicate = true;
		}

	}

	public IinqInsert getInsertParameters() {
		return insertParameters;
	}

	public boolean isDuplicate() {
		return duplicate;
	}

	private void generatePreparedFunction(ExecuteFunction executeFunction) throws InvalidArgumentException, SQLException {
		StringBuilder header = new StringBuilder();
		StringBuilder definition = new StringBuilder();

		LQInsertNode insertNode = (LQInsertNode) globalUpdate.getPlan().getLogicalQueryTree().getRoot();
		IinqTable table = (IinqTable) iinqDatabase.getIinqTable(insertNode.getSourceTable().getTable().getTableName());
		String table_name = table.getTableName().toLowerCase();

		this.setName("insert_" + table.getTableId());

		/* Number of fields specified for the insert */
		int count = insertNode.getInsertFields().size();
		int[] insert_field_nums = new int[count];

		/* Get field numbers for the insert */
		for (int i = 0; i < count; i++) {
			insert_field_nums[i] = table.getFieldPosition(insertNode.getInsertFields().get(i).getName().toLowerCase());
		}

		boolean[] prep_fields = new boolean[count];

		/* Check if the INSERT statement is a prepared statement */
		UnityPreparedStatement stmt = (UnityPreparedStatement) iinqDatabase.prepareUnityStatement(insertNode.generateSQL());
		preparedStatement = stmt.getParameters().size() > 0;

		String[] fields = new String[count];
		String field_value;

		String key_type = table.getSchemaValue(PRIMARY_KEY_TYPE);
		String key_field_num = table.getSchemaValue(PRIMARY_KEY_FIELD);

		ArrayList<Integer> int_fields = new ArrayList<>();
		ArrayList<Integer> string_fields = new ArrayList<>();

		ArrayList<String> field_values = new ArrayList<>();
		ArrayList<String> field_types = new ArrayList<>();
		ArrayList<String> iinq_field_types = new ArrayList<>();
		ArrayList<String> field_sizes = new ArrayList<>();

		header.append("iinq_prepared_sql ");
		header.append(getName());
		header.append("(");

		for (int j = 0; j < count; j++) {
			GQFieldRef fieldNode = (GQFieldRef) insertNode.getInsertFields().get(j);
			fields[j] = ((LQExprNode) insertNode.getInsertValues().get(j)).getContent().toString();

			field_value = fieldNode.getField().getDataTypeName();
			field_sizes.add(table.getIonFieldSize(fieldNode.getName()));

			/* To be added in function call */
			field_values.add(fields[j]);

			if (field_value.contains("CHAR")) {
				string_fields.add(j + 1);
			} else {
				int_fields.add(j + 1);
			}

			if (j > 0) {
				header.append(", ");
			}

			if (field_value.contains("CHAR")) {
				field_types.add("char");
				iinq_field_types.add("iinq_null_terminated_string");

				header.append("char *value_" + (j + 1));

			} else {
				field_types.add("int");
				iinq_field_types.add("iinq_int");

				header.append("int value_" + (j + 1));
			}

			prep_fields[j] = fields[j].equals("?");
		}

		header.append(")");
		definition.append(header.toString());
		header.append(";\n");
		definition.append(" {\n");
		definition.append("\tiinq_prepared_sql p = {0};\n");

		definition.append("\tp.table = malloc(sizeof(int));\n");
		definition.append("\t*(int *) p.table = " + table.getTableId() + ";\n");
		definition.append("\tp.value = malloc(" + table.getSchemaValue(VALUE_SIZE) + ");\n");
		definition.append("\tunsigned char\t*data = p.value;\n");
		definition.append("\tp.key = malloc(" + table.generateIonKeySize() + ");\n");

		// TODO: allow composite keys
		if (Integer.parseInt(key_type) == Types.INTEGER) {
			definition.append("\t*(int *) p.key = value_" + (Integer.parseInt(key_field_num)) + ";\n\n");
		} else {
			definition.append("\tmemcpy(p.key, value_" + (Integer.parseInt(key_field_num)) + ", " + key_type + ");\n\n");
		}

		for (int i = 0; i < fields.length; i++) {
			field_value = table.getSchemaValue(FIELD_TYPE, i+1);
			String value_size = table.getSchemaValue(FIELD_SIZE, i+1);

			if (field_value.contains("CHAR")) {
				definition.append("\tif (value_" + (i + 1) + " != NULL)\n");
				definition.append("\t\tmemcpy(data, value_" + (i + 1) + ", " + value_size + ");\n");
				definition.append("\telse\n");
				definition.append("\t\t*(char*) data = NULL;\n");
			} else {
				definition.append("\t*(int *) data = value_" + (i + 1) + ";\n");
			}

			if (i < fields.length - 1) {
				definition.append("\tdata += " + value_size + ";\n\n");
			}
		}

		definition.append("\n\treturn p;\n");
		definition.append("}\n\n");

		table_info = new tableInfo(table.getTableId(), count, iinq_field_types, field_sizes);

		String param_header = "";

		setDefinition(definition.toString());
		setHeader(header.toString());

		insertParameters = new IinqInsert(table.getTableId(), fields, new IinqInsertFields(field_values, field_types, insert_field_nums, table.getNumFields()), int_fields, string_fields, count, prep_fields, key_type, key_field_num);
	}

	public boolean isPreparedStatement() {
		return preparedStatement;
	}

	public static boolean isSetParamsWritten() {
		return setParamsWritten;
	}

	public SetPreparedParametersFunction createSetParams() {
		setParamsWritten = true;
		return new SetPreparedParametersFunction();
	}
}