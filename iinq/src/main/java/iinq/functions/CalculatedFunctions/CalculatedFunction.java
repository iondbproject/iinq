package iinq.functions.CalculatedFunctions;

import iinq.metadata.IinqTable;

public interface CalculatedFunction {
	public void addTable(IinqTable table);
	public String generateDefinition();
}
