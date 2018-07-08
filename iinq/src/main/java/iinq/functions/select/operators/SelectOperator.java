package iinq.functions.select.operators;

import iinq.functions.select.operators.destroy.OperatorDestroyFunction;
import iinq.functions.select.operators.init.OperatorInitFunction;
import iinq.functions.select.operators.next.OperatorNextFunction;

public abstract class SelectOperator {
	private OperatorInitFunction initFunction;
	private OperatorNextFunction nextFunction;
	private OperatorDestroyFunction destroyFunction;

	public SelectOperator(OperatorInitFunction initFunction, OperatorNextFunction nextFunction, OperatorDestroyFunction destroyFunction) {
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
}
