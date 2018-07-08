package iinq.functions.select.operators;

import iinq.functions.select.operators.destroy.TableScanDestroyFunction;
import iinq.functions.select.operators.init.TableScanInitFunction;
import iinq.functions.select.operators.next.TableScanNextFunction;

public class TableScanOperator extends SelectOperator {
	public TableScanOperator() {
		super(new TableScanInitFunction(), new TableScanNextFunction(), new TableScanDestroyFunction());
	}
}
