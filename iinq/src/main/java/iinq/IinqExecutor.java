package iinq;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.functions.CalculatedFunctions.ExecuteFunction;
import iinq.functions.PreparedInsertFunction;
import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import iinq.query.IinqBuilder;
import iinq.query.IinqQuery;
import unity.annotation.AnnotatedSourceDatabase;
import unity.jdbc.UnityPreparedStatement;
import unity.parser.GlobalParser;
import unity.query.*;
import unity.util.StringFunc;

import javax.management.relation.RelationNotFoundException;
import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

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
		String table_name = updateNode.getTable().getTable().getTableName().toLowerCase();

		boolean table_found = false;

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
			IinqBuilder builder = new IinqBuilder(kingParser.parse("SELECT * FROM " + table_name + " WHERE " + conditionNode.generateSQL() + ";", iinqDatabase.getSchema()).getLogicalQueryTree().getRoot(), iinqDatabase);
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
		ArrayList<Integer> update_field_types = new ArrayList<>();
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
				int field_type = table.getFieldType(n);
				field_sizes.add(table.getIonFieldSize(n));

				if (update_field.equalsIgnoreCase(table.getFieldName(n))) {
					update_field_nums.add(n);
					update_field_types.add(field_type);
				}
				if (implicit_field.equalsIgnoreCase(table.getFieldName(n))) {
					implicit_fields.add(n);
				}
			}
		}

		String key_size = table.getSchemaValue(PRIMARY_KEY_SIZE);
		String value_size = table.getSchemaValue(VALUE_SIZE);
		String ion_key = table.getSchemaValue(ION_KEY_TYPE);

		// TODO revise IinqUpdate to use IinqWhere object
		return new IinqUpdate(table.getTableId(), num_conditions, num_fields, new ArrayList<Integer>(Arrays.asList(where.getWhere_field_nums())), new ArrayList<String>(Arrays.asList(where.getWhere_operators())),
				new ArrayList<String>(Arrays.asList(where.getWhere_values())), new ArrayList<String>(Arrays.asList(where.getWhere_field_types())), key_size, value_size, ion_key, update_field_nums, implicit, implicit_fields, update_operators,
				update_values, update_field_types, implicit_count);
	}

	public IinqInsert executeInsertStatement(String sql) throws SQLException, InvalidArgumentException {
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != iinqDatabase.getSchema()) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, iinqDatabase.getSchema());
		} else {
			throw new SQLException("Metadata is required for inserts.");
		}

		LQInsertNode insertNode = (LQInsertNode) gu.getPlan().getLogicalQueryTree().getRoot();
		IinqTable table = iinqDatabase.getIinqTable(insertNode.getSourceTable().getTable().getTableName());
		UnityPreparedStatement stmt = iinqDatabase.prepareUnityStatement(sql);
		boolean prep = (stmt.getParameters().size() > 0);
		stmt.close();

		if (iinqDatabase.insertFunctionExists(table.getTableId())) {
			return new IinqInsert(table, insertNode, iinqDatabase.getInsertFunction(table.getTableId()), prep, true);
		} else {
			return new IinqInsert(table, insertNode, new PreparedInsertFunction(gu, iinqDatabase), prep, false);
		}
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
			IinqBuilder builder = new IinqBuilder(kingParser.parse("SELECT * FROM " + table_name + " WHERE " + conditionNode.generateSQL() + ";", iinqDatabase.getSchema()).getLogicalQueryTree().getRoot(), iinqDatabase);
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
		return new delete_fields(iinqTable.getTableId(), num_conditions, new ArrayList<Integer>(Arrays.asList(iinqWhere.getWhere_field_nums())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_operators())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_values())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_field_types())), key_size, value_size, ion_key);
	}

	public int executeDropTable(String sql) throws SQLException {
		// Use UnityJDBC to parse the drop table statement (metadata is required to verify table existence)
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != iinqDatabase.getSchema()) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, iinqDatabase.getSchema());
		} else {
			throw new SQLException("Metadata is required for dropping tables.");
		}

		String table_name = ((LQDropNode) gu.getPlan().getLogicalQueryTree().getRoot()).getName().toLowerCase();
		IinqTable table = iinqDatabase.getIinqTable(table_name);
		if (table == null) {
			throw new SQLException("Attempt to drop non-existent table: " + table_name);
		}

		/* Drop table from in-memory database */
		Statement stmt = iinqDatabase.createJavaStatement();
		stmt.execute(sql);
		stmt.close();

		iinqDatabase.getSchema().removeTable(iinqDatabase.getDatabaseName(), table_name);
		iinqDatabase.removeIinqTable(table);

		return table.getTableId();
	}

	public IinqSelect executeQuery(String sql) throws SQLException, RelationNotFoundException, IOException, InvalidArgumentException {
		// Parse semantic query string into a parse tree
		GlobalParser kingParser;
		GlobalQuery gq;
		if (null != iinqDatabase.getSchema()) {
			kingParser = new GlobalParser(false, true);
			gq = kingParser.parse(sql, iinqDatabase.getSchema());
		} else {
			throw new SQLException("Metadata needed for queries.");
		}
		gq.setQueryString(sql);

		// Optimize logical query tree before execution
		Optimizer opt = new Optimizer(gq, false, null);
		gq = opt.optimize();

		IinqBuilder builder = new IinqBuilder(gq.getLogicalQueryTree().getRoot(), iinqDatabase);
		IinqQuery query = builder.toQuery();
		HashMap<String, Object> code = query.generateCode();
		IinqWhere where = null;
		int num_conditions;
		where = (IinqWhere) code.get("where");

		IinqTable table = iinqDatabase.getIinqTable(query.getTableName());

		ArrayList<Integer> select_field_nums = new ArrayList<>();
		ArrayList<String> field_sizes = new ArrayList<>();

		ArrayList<String> fields = (ArrayList<String>) query.getParameterObject("fieldList");
		ArrayList<Integer> fieldNums = (ArrayList<Integer>) query.getParameterObject("fieldListNums");
		int num_fields = fields.size();

		//fields = get_fields(field_list, num_fields);

/*		for (int j = 0; j < num_fields; j++) {
			for (int n = 0; n < table.getNumFields(); n++) {
				String field_type = table.getFieldName(n + 1);
				field_sizes.add(table.getIonFieldSize(j + 1));

				if ((fields[j].trim()).equals(table.getFieldName(n + 1))) {
					select_field_nums.add(n + 1);
				}
			}
		}*/

		String value_size = table.generateIonValueSize();
		String key_size = table.generateIonKeySize();
		String ion_key = table.getIonKeyType();

		return new IinqSelect(table.getTableName(), table.getTableId(), num_fields, where, ion_key, key_size, value_size, fieldNums, null);
	}
}
