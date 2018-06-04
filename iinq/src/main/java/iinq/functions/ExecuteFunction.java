package iinq.functions;

import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ExecuteFunction extends IinqFunction {
	private HashMap<Integer, executeCallData> tableIdToFunctionCall = new HashMap<>();

	private class executeCallData {
		String tableName;
		String keyCast;

		private executeCallData(String tableName, String keyCast) {
			this.tableName = tableName;
			this.keyCast = keyCast;
		}
	}

	public ExecuteFunction() {
		super("execute", "void execute(iinq_prepared_sql p);\n", null);
	}

	public ExecuteFunction append(ExecuteFunction ex2) {
		this.tableIdToFunctionCall.putAll(ex2.tableIdToFunctionCall);
		return this;
	}

	public boolean containsCallData(IinqTable table) {
		return tableIdToFunctionCall.containsKey(table.getTableId());
	}

	public void addCallData(IinqTable iinqTable) {
		addCallData(iinqTable.getTableId(), iinqTable.getTableName(), iinqTable.getIonKeyType());
	}

	public void addCallData(int tableId, String tableName, String keyCast) {
		tableIdToFunctionCall.put(tableId, new executeCallData(tableName, keyCast));
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder("void execute(iinq_prepared_sql p) {\n" +
				"\tswitch (*(int*) p.table) {\n");
		for (Map.Entry<Integer, executeCallData> entry : tableIdToFunctionCall.entrySet()) {
			def.append(String.format("\t\tcase %d: {\n", entry.getKey()));
			def.append(String.format("\t\t\tiinq_execute(\"%s\", %s, p.value, iinq_insert_t);\n\t\t}\n", entry.getValue().tableName, entry.getValue().keyCast));
		}
		def.append("\t}\n" +
				"\tfree(p.value);\n" +
				"\tfree(p.table);\n" +
				"\tfree(p.key);\n" +
				"}\n\n");
		setDefinition(def.toString());
		return def.toString();
	}
}
