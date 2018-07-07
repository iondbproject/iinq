package iinq.functions.select.operators;

import iinq.functions.select.operators.destroy.OperatorDestroyFunction;
import iinq.functions.select.operators.next.OperatorNextFunction;

public abstract class SelectOperator {
	private OperatorNextFunction nextFunction;
	private OperatorDestroyFunction destroyFunction;

	public SelectOperator(OperatorNextFunction nextFunction, OperatorDestroyFunction destroyFunction) {
		this.nextFunction = nextFunction;
		this.destroyFunction = destroyFunction;
	}

	public OperatorDestroyFunction getDestroyFunction() {
		return destroyFunction;
	}

	public OperatorNextFunction getNextFunction() {
		return nextFunction;
	}

}
