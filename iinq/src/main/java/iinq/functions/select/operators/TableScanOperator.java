package iinq.functions.select.operators;

import iinq.functions.select.operators.destroy.TableScanDestroyFunction;
import iinq.functions.select.operators.init.TableScanInitFunction;
import iinq.functions.select.operators.next.TableScanNextFunction;
import iinq.functions.select.operators.struct.TableScanStruct;
import iinq.metadata.IinqTable;

public class TableScanOperator extends IinqOperator {
	protected int tableId;
	protected int numFields;

	public TableScanOperator(int tableId, int numFields) {
		super("table_scan", new TableScanInitFunction(), new TableScanNextFunction(), new TableScanDestroyFunction());
		this.tableId = tableId;
		this.numFields = numFields;
		this.struct = new TableScanStruct();
	}

	public TableScanOperator(IinqTable table) {
		this(table.getTableId(), table.getNumFields());
	}

	public String generateInitFunctionCall() {
		return String.format("%s(%d, %d)", getInitFunction().getName(), tableId, numFields);
	}
}
