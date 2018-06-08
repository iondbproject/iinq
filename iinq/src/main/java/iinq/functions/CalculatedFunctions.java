package iinq.functions;

import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.Iterator;

public class CalculatedFunctions {
	private ArrayList<IinqFunction> functions = new ArrayList<>();

	public CalculatedFunctions() {
		functions.add(new GetFieldTypeFunction());
		functions.add(new CalculateOffsetFunction());
		functions.add(new ExecuteFunction());
	}

	public void addTable(IinqTable table) {
		Iterator<IinqFunction> it = functions.iterator();
		while (it.hasNext()) {
			((CalculatedFunction) it.next()).addTable(table);
		}
	}

	public void generateDefinitions() {
		Iterator<IinqFunction> it = functions.iterator();
		while (it.hasNext()) {
			((CalculatedFunction) it.next()).generateDefinition();
		}
	}

	public ArrayList<IinqFunction> getFunctions() {
		return functions;
	}
}
