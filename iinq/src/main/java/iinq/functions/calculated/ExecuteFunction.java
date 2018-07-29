package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ExecuteFunction extends IinqFunction implements CalculatedFunction {
	private HashMap<Integer, String> tableIdToFunctionCall = new HashMap<>();

	public ExecuteFunction() {
		super("iinq_execute_prepared", "ion_err_t iinq_execute_prepared(iinq_prepared_sql *p);\n", null);
	}

	public ExecuteFunction append(ExecuteFunction ex2) {
		this.tableIdToFunctionCall.putAll(ex2.tableIdToFunctionCall);
		return this;
	}

	private boolean containsTable(IinqTable table) {
		return tableIdToFunctionCall.containsKey(table.getTableId());
	}

	public void addTable(IinqTable iinqTable) {
		StringBuilder keyCheck = new StringBuilder();
		ArrayList<Integer> keyIndices = iinqTable.getPrimaryKeyIndices();
		Iterator<Integer> it = keyIndices.iterator();
		keyCheck.append("\t\t\tif (");
		while (it.hasNext()) {
			keyCheck.append(String.format("iinq_check_null_indicator(p->value, %d) || ", it.next()));
		}
		keyCheck.setLength(keyCheck.length()-4);
		keyCheck.append(") {\n");
		keyCheck.append("\t\t\t\treturn err_unable_to_insert;\n" +
				"\t\t\t}\n");
		tableIdToFunctionCall.put(iinqTable.getTableId(), keyCheck.toString());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder("ion_err_t iinq_execute_prepared(iinq_prepared_sql *p) {\n" +
				"\tswitch (p->table) {\n");
		for (Map.Entry<Integer, String> entry : tableIdToFunctionCall.entrySet()) {
			def.append(String.format("\t\tcase %d: {\n", entry.getKey()));
			def.append(entry.getValue());
			def.append(String.format("\t\t\treturn iinq_execute(%d, p->key, p->value, iinq_insert_t);\n", entry.getKey()));
			def.append("\t\t}\n");
		}
		def.append("\t}\n" +
				"}\n\n");
		setDefinition(def.toString());
		return def.toString();
	}
}
