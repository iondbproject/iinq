package iinq.functions.select;

import iinq.functions.IinqFunction;
import iinq.functions.select.operators.IinqOperator;

import java.util.HashMap;
import java.util.Map;

public class SelectFunctions {
	private HashMap<String, IinqFunction> functions = new HashMap<>();
	public SelectFunctions(HashMap<String, IinqOperator> operators) {
		for (Map.Entry<String, IinqOperator> operatorEntry : operators.entrySet()) {

		}
	}

	public HashMap<String, IinqFunction> getFunctions() {
		return functions;
	}
}
