package iinq;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.callable.IinqDelete;
import iinq.callable.IinqInsert;
import iinq.callable.IinqProjection;
import iinq.callable.update.IinqUpdate;
import iinq.callable.update.IinqUpdateFieldList;
import iinq.callable.update.ImplicitFieldInfo;
import iinq.callable.update.UpdateField;
import iinq.functions.calculated.ExecuteFunction;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

public class IinqExecutor {
	private IinqDatabase iinqDatabase;
	private ExecuteFunction exFunc;

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

		IinqTable table = iinqDatabase.getIinqTable(table_name);

		if (table == null) {
			throw new SQLException("Update attempted on non-existent table: " + table_name);
		}

		LQCondNode conditionNode = updateNode.getCondition();
		ArrayList<String> predicates = null;
		int num_conditions = 0;
		if (conditionNode != null) {
			LQSelNode selNode = new LQSelNode();
			selNode.setCondition(conditionNode);
			// TODO: rewrite condition for iinq statements
			IinqBuilder builder = new IinqBuilder(kingParser.parse("SELECT * FROM " + table_name + " WHERE " + conditionNode.generateSQL() + ";", iinqDatabase.getSchema()).getLogicalQueryTree().getRoot(), iinqDatabase);
			IinqQuery query = builder.toQuery();
			Object filters = query.getParameterObject("filter");
			if (filters instanceof ArrayList) {
				predicates = (ArrayList) filters;
				num_conditions = predicates.size();
			} else if (filters instanceof String) {
				num_conditions = 1;
				predicates = new ArrayList<>();
				predicates.add((String) filters);
			}
		}

		/* Calculate number of fields to update in statement */
		int num_fields = updateNode.getNumFields();

		IinqSelection where = new IinqSelection(predicates, table);

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
		String update_value;

		IinqUpdateFieldList fieldList = new IinqUpdateFieldList();
		for (int j = 0; j < num_fields; j++) {
			is_implicit = false;
			update_field = fields[j].trim();
			update_value = fieldValues[j].getContent().toString();
			String updateOperator = null;

			/* Check if update value contains an operatorType */
			if (fieldValues[j].getContent().equals("+")) {
				updateOperator = "iinq_add";
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			} else if (fieldValues[j].getContent().equals("-")) {
				updateOperator = "iinq_subtract";
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			} else if (fieldValues[j].getContent().equals("*")) {
				updateOperator = "iinq_multiply";
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			} else if (fieldValues[j].getContent().equals("/")) {
				updateOperator = "iinq_divide";
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
				is_implicit = true;
			}

			int fieldNum = table.getFieldPosition(update_field);
			int fieldType = table.getFieldType(fieldNum);
			if (fieldType == Types.INTEGER) {
				fieldList.addField(new UpdateField(fieldNum, is_implicit ? new ImplicitFieldInfo(table.getFieldPosition(implicit_field), updateOperator) : null, Integer.parseInt(update_value)));
			} else {
				fieldList.addField(new UpdateField(fieldNum, is_implicit ? new ImplicitFieldInfo(table.getFieldPosition(implicit_field), updateOperator) : null, update_value.replace("\'", "\"")));
			}

		}

		return new IinqUpdate(table.getTableId(), where, num_fields, fieldList);
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

	public IinqDelete executeDeleteStatement(String sql) throws SQLException, InvalidArgumentException, IOException, RelationNotFoundException {
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

		IinqSelection iinqSelection = new IinqSelection(conditions, iinqTable);

		return new IinqDelete(iinqTable.getTableId(), iinqSelection);
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

	public IinqQuery executeQuery(String sql) throws SQLException, RelationNotFoundException, IOException, InvalidArgumentException {
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
		gq = opt.optimize();gq.printTree();

		IinqBuilder builder = new IinqBuilder(gq.getLogicalQueryTree().getRoot(), iinqDatabase);
		IinqQuery query = builder.toQuery();
		HashMap<String, Object> code = query.generateCode();

		IinqTable table = iinqDatabase.getIinqTable(query.getTableName());

		ArrayList<Integer> fieldNums = (ArrayList<Integer>) query.getParameterObject("fieldListNums");

//		String project_size = table.generateProjectionSize(fieldNums);

		return query;
	}
}
