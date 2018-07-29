package iinq.functions.select.operators;

import iinq.IinqSelection;
import iinq.functions.select.operators.destroy.SelectionDestroyFunction;
import iinq.functions.select.operators.init.SelectionInitFunction;
import iinq.functions.select.operators.next.SelectionNextFunction;
import iinq.functions.select.operators.struct.SelectionStruct;
import iinq.metadata.IinqTable;

public class SelectionOperator extends IinqOperator {
	protected IinqSelection selection;

	public SelectionOperator(IinqOperator inputOperator) {
		super("iinq_selection", new SelectionInitFunction(), new SelectionNextFunction(), new SelectionDestroyFunction());
		struct = new SelectionStruct();
		inputOperators.add(inputOperator);
		selection = new IinqSelection();
	}

	public String generateInitFunctionCall() {
		return String.format("%s(%s, %d, %s)", getInitFunction().getName(), inputOperators.get(0).generateInitFunctionCall(), selection.getNumConditions(), selection.generateIinqConditionList());
	}

	public void addCondition(String condition, IinqTable table) {
		selection.addCondition(condition, table);
	}
}
