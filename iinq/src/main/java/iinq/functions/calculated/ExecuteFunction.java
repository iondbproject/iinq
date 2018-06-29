package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.HashMap;
import java.util.Map;

public class ExecuteFunction extends IinqFunction implements CalculatedFunction {
	private HashMap<Integer, executeCallData> tableIdToFunctionCall = new HashMap<>();

	private class executeCallData {
		int tableId;

		private executeCallData(int tableId) {
			this.tableId = tableId;
		}
	}

	public ExecuteFunction() {
		super("execute", "void execute(iinq_prepared_sql *p);\n", null);
	}

	public ExecuteFunction append(ExecuteFunction ex2) {
		this.tableIdToFunctionCall.putAll(ex2.tableIdToFunctionCall);
		return this;
	}

	private boolean containsTable(IinqTable table) {
		return tableIdToFunctionCall.containsKey(table.getTableId());
	}

	public void addTable(IinqTable iinqTable) {
		if (!containsTable(iinqTable))
			addCallData(iinqTable.getTableId());
	}

	private void addCallData(int tableId) {
		tableIdToFunctionCall.put(tableId, new executeCallData(tableId));
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder("void execute(iinq_prepared_sql *p) {\n" +
				"\tswitch (p->table) {\n");
		for (Map.Entry<Integer, executeCallData> entry : tableIdToFunctionCall.entrySet()) {
			def.append(String.format("\t\tcase %d: {\n", entry.getKey()));
			def.append(String.format("\t\t\tiinq_execute(%d, p->key, p->value, iinq_insert_t);\n", entry.getValue().tableId));
			def.append("\t\t\tbreak;\n\t\t}\n");
		}
		def.append("\t}\n" +
				"}\n\n");
		setDefinition(def.toString());
		return def.toString();
	}
}
