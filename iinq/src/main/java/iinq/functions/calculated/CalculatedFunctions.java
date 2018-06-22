package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CalculatedFunctions {
	private HashMap<String, IinqFunction> functions = new HashMap<>();

	public CalculatedFunctions() {
		IinqFunction function;
		function = new GetFieldTypeFunction();
		functions.put(function.getName(), function);
		function = new CalculateOffsetFunction();
		functions.put(function.getName(), function);
		function = new ExecuteFunction();
		functions.put(function.getName(), function);
		function = new CalculatedPrintFunction();
		functions.put(function.getName(), function);
	}

	public void addTable(IinqTable table) {
		Iterator<Map.Entry<String, IinqFunction>> it = functions.entrySet().iterator();
		while (it.hasNext()) {
			((CalculatedFunction) it.next().getValue()).addTable(table);
		}
	}

	public void generateDefinitions() {
		Iterator<Map.Entry<String, IinqFunction>> it = functions.entrySet().iterator();
		while (it.hasNext()) {
			((CalculatedFunction) it.next().getValue()).generateDefinition();
		}
	}

	public HashMap<String, IinqFunction> getFunctions() {
		return functions;
	}
}
