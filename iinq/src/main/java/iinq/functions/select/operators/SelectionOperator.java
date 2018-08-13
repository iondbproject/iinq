package iinq.functions.select.operators;

import iinq.IinqSelection;
import iinq.IinqSelectionPredicate;
import iinq.functions.select.operators.destroy.SelectionDestroyFunction;
import iinq.functions.select.operators.init.SelectionInitFunction;
import iinq.functions.select.operators.next.SelectionNextFunction;
import iinq.functions.select.operators.predicates.IonPredicate;
import iinq.functions.select.operators.struct.SelectionStruct;
import iinq.metadata.IinqTable;

import java.util.ArrayList;

public class SelectionOperator extends IinqOperator {
	protected IinqSelection selection;

	public SelectionOperator(IinqOperator inputOperator) {
		super("iinq_selection", new SelectionInitFunction(), new SelectionNextFunction(), new SelectionDestroyFunction());
		struct = new SelectionStruct();
		inputOperators.add(inputOperator);
		selection = new IinqSelection();
	}

	public String generateInitFunctionCall() {
		if (selection.getNumPredicates() > 0)
			return String.format("%s(%s, %d, %s)", getInitFunction().getName(), inputOperators.get(0).generateInitFunctionCall(), selection.getNumPredicates(), selection.toIinqConditionListString());
		else
			return inputOperators.get(0).generateInitFunctionCall();
	}

	public void addPredicate(String predicate, IinqTable table) {
		selection.addPredicate(predicate, table);
	}

	public boolean containsKeyPredicate() {
		return selection.containsKeyPredicate();
	}

	public IonPredicate optimizePredicate() {
		return selection.optimizePredicate();
	}
}
