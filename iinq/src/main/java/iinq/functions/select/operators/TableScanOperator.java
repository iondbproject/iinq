package iinq.functions.select.operators;

import iinq.functions.select.operators.destroy.TableScanDestroyFunction;
import iinq.functions.select.operators.init.TableScanInitFunction;
import iinq.functions.select.operators.next.TableScanNextFunction;

public class TableScanOperator extends SelectOperator {
	public TableScanOperator() {
		super(new TableScanInitFunction(), new TableScanNextFunction(), new TableScanDestroyFunction());
	}

	public String generateInitFunctionCall() {
		StringBuilder functionCall = new StringBuilder();

		functionCall.append(String.format("%s = %s(%d, %d, %d", iinqSelect.return_value, iinqSelect.operator.getInitFunction().getName(), iinqSelect.table_id, iinqSelect.num_wheres, iinqSelect.num_fields));

		if (iinqSelect.num_wheres > 0) {
			functionCall.append(", ");
			functionCall.append(iinqSelect.where.generateIinqConditionList());
		}

		functionCall.append(", IINQ_SELECT_LIST(");
		for (int i = 0; i < iinqSelect.num_fields; i++) {
			functionCall.append(iinqSelect.fields.get(i)).append(", ");
		}
		functionCall.setLength(functionCall.length() - 2);

		functionCall.append("));\n");

		return functionCall.toString();
	}
}
