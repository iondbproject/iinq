package iinq;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.functions.ExecuteFunction;
import iinq.functions.PreparedInsertFunction;
import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import unity.annotation.AnnotatedSourceDatabase;
import unity.parser.GlobalParser;
import unity.query.*;
import unity.util.StringFunc;

import javax.management.relation.RelationNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import static iinq.functions.SchemaKeyword.*;
import static iinq.functions.SchemaKeyword.ION_KEY_TYPE;
import static iinq.functions.SchemaKeyword.VALUE_SIZE;

public class IinqExecutor {
	protected IinqDatabase iinqDatabase;
	protected ExecuteFunction exFunc;

	public IinqExecutor(IinqDatabase iinqDatabase) {
		this.iinqDatabase = iinqDatabase;
		this.exFunc = new ExecuteFunction();
	}

	public IinqTable executeCreateTable(String sql) throws SQLException, IOException {
		AnnotatedSourceDatabase db = iinqDatabase.getUnityDB();

		Statement stmt = iinqDatabase.createJavaStatement();
		sql = StringFunc.verifyTerminator(sql);
		stmt.execute(sql);
		stmt.close();
		iinqDatabase.updateSourceTables();
		IinqTable iinqTable = iinqDatabase.getNewlyCreatedIinqTable();
		iinqTable.setCreateTableStatement(sql);
		iinqDatabase.addIinqTable(iinqTable);

		db.updateSchemaFile();

		return iinqTable;
	}

	public IinqUpdate executeUpdateStatement(String sql) throws SQLException, RelationNotFoundException, IOException, InvalidArgumentException {
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != iinqDatabase.getSchema()) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, iinqDatabase.getSchema());
		} else {
			throw new SQLException("Metadata is required for updating tables.");
		}

		LQUpdateNode updateNode = (LQUpdateNode) gu.getPlan().getLogicalQueryTree().getRoot();
		String table_name = updateNode.getTable().getTable().getTableName();

		boolean table_found = false;
		int table_id = 0;

		IinqTable table = iinqDatabase.getIinqTable(table_name);

		if (table == null) {
			throw new SQLException("Update attempted on non-existent table: " + table_name);
		}

		LQCondNode conditionNode = updateNode.getCondition();
		ArrayList<String> conditions = null;
		int num_conditions = 0;
		if (conditionNode != null) {
			LQSelNode selNode = new LQSelNode();
			selNode.setCondition(conditionNode);
			// TODO: rewrite condition for iinq statements
			IinqBuilder builder = new IinqBuilder(kingParser.parse("SELECT * FROM " + table_name + " WHERE " + conditionNode.generateSQL() + ";", iinqDatabase.getSchema()).getLogicalQueryTree().getRoot());
			IinqQuery query = builder.toQuery();
			Object filters = query.getParameterObject("filter");
			if (filters instanceof ArrayList) {
				conditions = (ArrayList) filters;
				num_conditions = conditions.size();
			} else if (filters instanceof String) {
				num_conditions = 1;
				conditions = new ArrayList<>();
				conditions.add((String) filters);
			}
		}

		String[] conditionFields = new String[num_conditions];

		for (int i = 0; i < num_conditions; i++) {
			conditionFields[i] = conditions.get(i);
		}

		/* Get fields to update */
		String update;

		/* Calculate number of fields to update in statement */
		int num_fields = updateNode.getNumFields();

		IinqWhere where = new IinqWhere(num_conditions);
		where.generateWhere(conditionFields, table);

		ArrayList<Integer> update_field_nums = new ArrayList<>();
		ArrayList<Boolean> implicit = new ArrayList<>();
		ArrayList<Integer> implicit_fields = new ArrayList<>();
		ArrayList<String> update_operators = new ArrayList<>();
		ArrayList<String> update_values = new ArrayList<>();
		ArrayList<String> update_field_types = new ArrayList<>();
		ArrayList<String> field_sizes = new ArrayList<>();

		String[] fields = new String[updateNode.getNumFields()];
		LQExprNode[] fieldValues = new LQExprNode[updateNode.getNumFields()];
		for (int i = 0; i < fields.length; i++) {
			LQExprNode node = ((LQExprNode) updateNode.getField(i));
			fields[i] = node.getContent().toString();
			fieldValues[i] = (LQExprNode) updateNode.getValue(i);
		}

		String update_field;
		String implicit_field = "";
		boolean is_implicit;
		String update_value = null;
		int implicit_count = 0;

		for (int j = 0; j < num_fields; j++) {
			is_implicit = false;
			update_field = fields[j].trim();
			update_value = fieldValues[j].getContent().toString();

			/* Check if update value contains an operator */
			if (fieldValues[j].getContent().equals("+")) {
				update_operators.add("iinq_add");
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			} else if (fieldValues[j].getContent().equals("-")) {
				update_operators.add("iinq_subtract");
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			} else if (fieldValues[j].getContent().equals("*")) {
				update_operators.add("iinq_multiply");
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			} else if (fieldValues[j].getContent().equals("/")) {
				update_operators.add("iinq_divide");
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			}

			update_values.add(update_value);
			implicit.add(is_implicit);

			if (is_implicit) {
				implicit_count++;
			}

			for (int n = 1, m = table.getNumFields(); n <= m; n++) {
				String field_type = iinqDatabase.getSchemaValue(table_name, FIELD_TYPE, n);
				field_sizes.add(table.getIonFieldSize(n));

				if (update_field.equalsIgnoreCase(iinqDatabase.getSchemaValue(table_name, FIELD_NAME, n))) {
					update_field_nums.add(n + 1);
					update_field_types.add(field_type);
				}
				if (implicit_field.equalsIgnoreCase(iinqDatabase.getSchemaValue(table_name, FIELD_NAME, n))) {
					implicit_fields.add(n + 1);
				}
			}
		}

		String key_size = table.getSchemaValue(PRIMARY_KEY_SIZE);
		String value_size = table.getSchemaValue(VALUE_SIZE);
		String ion_key = table.getSchemaValue(ION_KEY_TYPE);

		// TODO revise IinqUpdate to use IinqWhere object
		return new IinqUpdate(table_name, table_id, num_conditions, num_fields, new ArrayList<Integer>(Arrays.asList(where.getWhere_field_nums())), new ArrayList<String>(Arrays.asList(where.getWhere_operators())),
				new ArrayList<String>(Arrays.asList(where.getWhere_values())), new ArrayList<String>(Arrays.asList(where.getWhere_field_types())), key_size, value_size, ion_key, update_field_nums, implicit, implicit_fields, update_operators,
				update_values, update_field_types, implicit_count);
	}

	public PreparedInsertFunction executeInsertStatement(String sql) throws SQLException, InvalidArgumentException {
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != iinqDatabase.getSchema()) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, iinqDatabase.getSchema());
		} else {
			throw new SQLException("Metadata is required for inserts.");
		}

		PreparedInsertFunction insertFunction = new PreparedInsertFunction(gu, iinqDatabase, exFunc);

		return insertFunction;
	}

	public String generateExecuteFunction() {
		return exFunc.generateDefinition();
	}

	public ExecuteFunction getExFunc() {
		return exFunc;
	}

	public delete_fields executeDeleteStatement(String sql) throws SQLException, InvalidArgumentException, IOException, RelationNotFoundException {
		// Use UnityJDBC to parse the drop table statement (metadata is required to verify table existence)
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != iinqDatabase.getSchema()) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, iinqDatabase.getSchema());
		} else {
			throw new SQLException("Metadata is required for dropping tables.");
		}
		LQDeleteNode delete = (LQDeleteNode) gu.getPlan().getLogicalQueryTree().getRoot();
		String table_name = delete.getSourceTable().getTable().getTableName().toLowerCase();

		// TODO: update this after writing new buildCondition method for IinqBuilder
		LQCondNode conditionNode = delete.getCondition();
		ArrayList<String> conditions = null;
		int num_conditions = 0;
		if (conditionNode != null) {
			LQSelNode selNode = new LQSelNode();
			selNode.setCondition(conditionNode);
			IinqBuilder builder = new IinqBuilder(kingParser.parse("SELECT * FROM " + table_name + " WHERE " + conditionNode.generateSQL() + ";", iinqDatabase.getSchema()).getLogicalQueryTree().getRoot());
			IinqQuery query = builder.toQuery();
			Object filters = query.getParameterObject("filter");
			if (filters instanceof ArrayList) {
				conditions = (ArrayList) filters;
				num_conditions = conditions.size();
			} else if (filters instanceof String) {
				num_conditions = 1;
				conditions = new ArrayList<>();
				conditions.add((String) filters);
			}
		}

		IinqTable iinqTable = iinqDatabase.getIinqTable(table_name);
		if (null == iinqTable) {
			throw new SQLException("Delete attempted on non-existent table: " + table_name);
		}

		/* Create print table method if it doesn't already exist */
/*		if (!iinqTable.isPrintFunctionWritten()) {
			print_table(out, table_name);
			iinqTable.setPrintFunctionWritten(true);
		}*/

		/* Write function to file */
		String key_size = iinqTable.getSchemaValue(PRIMARY_KEY_SIZE);
		String value_size = iinqTable.getSchemaValue(VALUE_SIZE);

		String[] conditionFields = new String[num_conditions];

		for (int i = 0; i < num_conditions; i++) {
			conditionFields[i] = conditions.get(i);
		}

		IinqWhere iinqWhere = new IinqWhere(num_conditions);
		iinqWhere.generateWhere(conditionFields, iinqTable);

/*		if (new_table) {
			// TODO: update tableInfo constructor to take IinqWhere object a a parameter
			tableInfo table_info = new tableInfo(table_id, Integer.parseInt(iinqDatabase.getSchemaValue(table_name, NUMBER_OF_FIELDS)), new ArrayList(Arrays.asList(iinqWhere.getIinq_field_types())), new ArrayList(Arrays.asList(iinqWhere.getField_sizes())));

			calculateInfo.add(table_info);
			//tables_count++;
		}*/

		String ion_key = iinqTable.getSchemaValue(ION_KEY_TYPE);

		// TODO: update delete_fields to take an IinqWhere object as a parameter
		return new delete_fields(table_name, iinqTable.getTableId(), num_conditions, new ArrayList<Integer>(Arrays.asList(iinqWhere.getWhere_field_nums())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_operators())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_values())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_field_types())), key_size, value_size, ion_key);


	}
}
