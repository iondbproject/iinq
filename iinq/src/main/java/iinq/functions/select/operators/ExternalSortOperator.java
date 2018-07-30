package iinq.functions.select.operators;

import iinq.callable.IinqProjection;
import iinq.functions.select.operators.destroy.ExternalSortDestroyFunction;
import iinq.functions.select.operators.init.ExternalSortInitFunction;
import iinq.functions.select.operators.next.ExternalSortNextFunction;
import iinq.functions.select.operators.struct.ExternalSortStruct;
import iinq.query.IinqSort;

public class ExternalSortOperator extends IinqOperator {
	public IinqSort iinqSort;

	public ExternalSortOperator(IinqSort iinqSort, IinqOperator inputOperator) {
		super("external_sort", new ExternalSortInitFunction(), new ExternalSortNextFunction(), new ExternalSortDestroyFunction());
		this.iinqSort = iinqSort;
		this.inputOperators.add(inputOperator);
		this.struct = new ExternalSortStruct();
	}

	public String generateInitFunctionCall() {
		return String.format("%s(%s, %d, %s)", getInitFunction().getName(), inputOperators.get(0).generateInitFunctionCall(), iinqSort.getNumSortElements(), iinqSort.generateSortArray());
	}
}
