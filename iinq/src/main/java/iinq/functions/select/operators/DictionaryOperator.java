package iinq.functions.select.operators;

import iinq.functions.select.operators.destroy.DictionaryDestroyFunction;
import iinq.functions.select.operators.init.DictionaryInitFunction;
import iinq.functions.select.operators.next.DictionaryNextFunction;
import iinq.functions.select.operators.predicates.AllRecordsPredicate;
import iinq.functions.select.operators.predicates.DictionaryPredicates;
import iinq.functions.select.operators.predicates.IonPredicate;
import iinq.functions.select.operators.struct.DictionaryStruct;
import iinq.metadata.IinqTable;

public class DictionaryOperator extends IinqOperator implements DictionaryPredicates {
	protected int tableId;
	protected int numFields;
	protected IonPredicate predicate;

	public DictionaryOperator(int tableId, int numFields, IonPredicate predicate) {
		super("dictionary_operator", new DictionaryInitFunction(), new DictionaryNextFunction(), new DictionaryDestroyFunction());
		this.tableId = tableId;
		this.numFields = numFields;
		this.struct = new DictionaryStruct();
		this.predicate = predicate;
	}

	public DictionaryOperator(IinqTable table) {
		this(table.getTableId(), table.getNumFields(), new AllRecordsPredicate());
	}

	public String generateInitFunctionCall() {
		return String.format("%s(%d, %d, %s)", getInitFunction().getName(), tableId, numFields, predicate.getPredicateArguments());
	}

	public void optimizePredicate(SelectionOperator selectionOperator) {
		predicate = selectionOperator.optimizePredicate();
	}

	public int getPredicateType() {
		return predicate.getPredicateType();
	}
}
