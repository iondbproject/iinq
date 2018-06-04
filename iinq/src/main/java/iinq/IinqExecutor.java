package iinq;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.functions.ExecuteFunction;
import iinq.functions.PreparedInsertFunction;
import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import unity.annotation.AnnotatedSourceDatabase;
import unity.annotation.SourceTable;
import unity.parser.GlobalParser;
import unity.query.GlobalUpdate;
import unity.util.StringFunc;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;

public class IinqExecutor {
	protected IinqDatabase iinqDatabase;
	protected ExecuteFunction exFunc;

	public IinqExecutor(IinqDatabase iinqDatabase) {
		this.iinqDatabase = iinqDatabase;
		this.exFunc = new ExecuteFunction();
	}

	public IinqTable executeCreateTable(String sql) throws SQLException, IOException {
		AnnotatedSourceDatabase db = iinqDatabase.getUnityDB();
		HashMap<String, SourceTable> iinqTables = db.getSourceTables();
		Statement stmt = iinqDatabase.createJavaStatement();
		sql = StringFunc.verifyTerminator(sql);
		stmt.execute(sql);
		stmt.close();
		iinqDatabase.updateSourceTables();
		IinqTable iinqTable = iinqDatabase.getNewlyCreatedIinqTable();
		iinqTable.setCreateTableStatement(sql);
		iinqTables.put(iinqTable.getTableName().toLowerCase(), iinqTable);

		db.updateSchemaFile();

		return iinqTable;
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
}
