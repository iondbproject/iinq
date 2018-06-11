package iinq.functions;

import iinq.metadata.IinqTable;

import java.util.HashMap;
import java.util.Map;

public class ExecuteFunction extends IinqFunction implements CalculatedFunction {
	private HashMap<Integer, executeCallData> tableIdToFunctionCall = new HashMap<>();

	private class executeCallData {
		int tableId;
		String keyCast;

		private executeCallData(int tableId, String keyCast) {
			this.tableId = tableId;
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

	public boolean containsTable(IinqTable table) {
		return tableIdToFunctionCall.containsKey(table.getTableId());
	}

	public void addTable(IinqTable iinqTable) {
		if (!containsTable(iinqTable))
			addCallData(iinqTable.getTableId(), iinqTable.getTableName(), iinqTable.getIonKeyCast());
	}

	private void addCallData(int tableId, String tableName, String keyCast) {
		tableIdToFunctionCall.put(tableId, new executeCallData(tableId, keyCast));
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder("void execute(iinq_prepared_sql p) {\n" +
				"\tswitch (*(int*) p.table) {\n");
		for (Map.Entry<Integer, executeCallData> entry : tableIdToFunctionCall.entrySet()) {
			def.append(String.format("\t\tcase %d: {\n", entry.getKey()));
			def.append(String.format("\t\t\tiinq_execute(%d, %s, p.value, iinq_insert_t);\n", entry.getValue().tableId, entry.getValue().keyCast));
			def.append("\t\t\tbreak;\n\t\t}\n");
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
