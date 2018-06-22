package iinq.functions.calculated;

import iinq.metadata.IinqTable;

interface CalculatedFunction {
	void addTable(IinqTable table);
	String generateDefinition();
}
