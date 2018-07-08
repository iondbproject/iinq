package iinq.functions.select;

import iinq.functions.IinqFunction;
import iinq.functions.select.operators.destroy.TableScanDestroyFunction;
import iinq.functions.select.operators.init.TableScanInitFunction;
import iinq.functions.select.operators.next.TableScanNextFunction;

import java.util.HashMap;

public class SelectFunctions {
	private HashMap<String, IinqFunction> functions = new HashMap<>();
	public SelectFunctions() {
		IinqFunction function;
/*		function = new GetIntFunction();
		functions.put(function.getName(), function);
		function = new GetStringFunction();
		functions.put(function.getName(), function); */
		function = new TableScanInitFunction();
		functions.put(function.getName(), function);
		// TODO: determine what next functions to generate
		function = new TableScanNextFunction();
		functions.put(function.getName(), function);
		function = new TableScanDestroyFunction();
		functions.put(function.getName(), function);
	}

	public HashMap<String, IinqFunction> getFunctions() {
		return functions;
	}
}
