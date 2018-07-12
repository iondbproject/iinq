package iinq.functions.select.operators;

import iinq.callable.IinqSelect;
import iinq.functions.select.operators.destroy.OperatorDestroyFunction;
import iinq.functions.select.operators.init.OperatorInitFunction;
import iinq.functions.select.operators.next.OperatorNextFunction;

public abstract class SelectOperator {
	protected OperatorInitFunction initFunction;
	protected OperatorNextFunction nextFunction;
	protected OperatorDestroyFunction destroyFunction;
	protected IinqSelect iinqSelect;

	public SelectOperator(OperatorInitFunction initFunction, OperatorNextFunction nextFunction, OperatorDestroyFunction destroyFunction) {
		this.initFunction = initFunction;
		this.nextFunction = nextFunction;
		this.destroyFunction = destroyFunction;
	}

	public void setIinqSelect(IinqSelect iinqSelect) {
		this.iinqSelect = iinqSelect;
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

	public abstract String generateInitFunctionCall();
}
