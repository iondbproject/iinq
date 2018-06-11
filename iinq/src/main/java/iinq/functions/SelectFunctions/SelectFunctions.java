package iinq.functions.SelectFunctions;

import iinq.functions.IinqFunction;

import java.util.HashMap;

public class SelectFunctions {
	private HashMap<String, IinqFunction> functions = new HashMap<>();
	public SelectFunctions() {
		IinqFunction function;
		function = new GetIntFunction();
		functions.put(function.getName(), function);
		function = new GetStringFunction();
		functions.put(function.getName(), function);
		function = new NextFunction();
		functions.put(function.getName(), function);
		function = new SelectFunction();
		functions.put(function.getName(), function);
	}

	public HashMap<String, IinqFunction> getFunctions() {
		return functions;
	}
}
