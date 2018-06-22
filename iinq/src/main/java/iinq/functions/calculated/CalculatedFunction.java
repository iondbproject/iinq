package iinq.functions.calculated;

import iinq.metadata.IinqTable;

public interface CalculatedFunction {
	public void addTable(IinqTable table);
	public String generateDefinition();
}
