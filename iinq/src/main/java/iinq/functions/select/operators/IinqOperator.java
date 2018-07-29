package iinq.functions.select.operators;

import iinq.callable.IinqProjection;
import iinq.functions.IinqFunction;
import iinq.functions.select.operators.destroy.OperatorDestroyFunction;
import iinq.functions.select.operators.init.OperatorInitFunction;
import iinq.functions.select.operators.next.OperatorNextFunction;
import iinq.functions.select.operators.struct.OperatorStruct;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class IinqOperator {
	protected String name;
	protected OperatorInitFunction initFunction;
	protected OperatorNextFunction nextFunction;
	protected OperatorDestroyFunction destroyFunction;
	protected ArrayList<IinqOperator> inputOperators = new ArrayList<>();
	protected OperatorStruct struct;

	public IinqOperator(String name, OperatorInitFunction initFunction, OperatorNextFunction nextFunction, OperatorDestroyFunction destroyFunction) {
		this.name = name;
		this.initFunction = initFunction;
		this.nextFunction = nextFunction;
		this.destroyFunction = destroyFunction;
	}

	public OperatorDestroyFunction getDestroyFunction() {
		return destroyFunction;
	}

	public OperatorNextFunction getNextFunction() {
		return nextFunction;
	}

	public OperatorInitFunction getInitFunction() {
		return initFunction;
	}

	public ArrayList<IinqOperator> getInputOperators() {
		return inputOperators;
	}

	public HashMap<String, IinqFunction> getOperatorFunctions() {
		HashMap<String, IinqFunction> operatorFunctions = new HashMap<>();

		operatorFunctions.put(initFunction.getName(), initFunction);
		operatorFunctions.put(nextFunction.getName(), nextFunction);
		operatorFunctions.put(destroyFunction.getName(), destroyFunction);

		for (IinqOperator inputOperator : inputOperators) {
			operatorFunctions.putAll(inputOperator.getOperatorFunctions());
		}

		return operatorFunctions;
	}

	public abstract String generateInitFunctionCall();

	public String getName() {
		return this.name;
	}

	public HashMap<String, String> getStructDefinitions() {
		HashMap<String, String> structDefinitions = new HashMap<>();

		structDefinitions.put(struct.getName(), struct.generateStructDefinition());

		for (IinqOperator inputOperator : inputOperators) {
			structDefinitions.putAll(inputOperator.getStructDefinitions());
		}

		return structDefinitions;
	}
}
