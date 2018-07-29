package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CalculatedFunctions {
	private HashMap<String, IinqFunction> functions = new HashMap<>();

	public CalculatedFunctions(boolean debug) {
		IinqFunction function;
		function = new GetFieldTypeFunction();
		functions.put(function.getName(), function);
		function = new CalculateOffsetFunction();
		functions.put(function.getName(), function);
		function = new ExecuteFunction();
		functions.put(function.getName(), function);
		function = new IsKeyFieldFunction();
		functions.put(function.getName(), function);
		function = new GetKeyFieldOffsetFunction();
		functions.put(function.getName(), function);

		if (debug) {
			functions.putAll(getDebugFunctions());
		}
	}

	public CalculatedFunctions() {
		this(false);
	}

	/* Extra functions used for debugging */
	public HashMap<String, IinqFunction> getDebugFunctions() {
		HashMap<String, IinqFunction> debugFunctions = new HashMap<>();
		IinqFunction function;

		function = new CalculatedPrintFunction();
		debugFunctions.put(function.getName(), function);
		function = new PrintKeysFunction();
		debugFunctions.put(function.getName(), function);

		return debugFunctions;
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
