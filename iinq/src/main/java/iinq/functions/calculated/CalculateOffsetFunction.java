package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.Iterator;

public class CalculateOffsetFunction extends IinqFunction implements CalculatedFunction {
	private ArrayList<String> tableOffsets = new ArrayList<>();

	public CalculateOffsetFunction() {
		super("calculateOffset",
		"size_t calculateOffset(iinq_table_id tableId, iinq_field_num_t field_num);\n",
				null);
	}

	public void addTable(IinqTable table) {
		StringBuilder offset = new StringBuilder();
		StringBuilder total = new StringBuilder();
		offset.append("\t\tcase ").append(table.getTableId()).append(": {\n");
		offset.append("\t\t\tswitch (field_num) {\n");
		offset.append("\t\t\t\tcase 1:\n");
		offset.append("\t\t\t\t\treturn 0;\n");
		for (int i = 1, n = table.getNumFields(); i <= n; i++) {
			offset.append("\t\t\t\tcase ").append(i+1).append(":\n");
			offset.append("\t\t\t\t\treturn ");
			total.append(table.getIonFieldSize(i));
			offset.append(total.toString());
			offset.append(";\n");
			total.append(" + ");
		}
		offset.append("\t\t\t\tdefault:\n\t\t\t\t\treturn 0;\n");
		offset.append("\t\t\t}\n\t\t}\n");

		tableOffsets.add(offset.toString());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();
		def.append("size_t calculateOffset(iinq_table_id tableId, iinq_field_num_t field_num) {\n\n");
		def.append("\tswitch (tableId) {\n");
		Iterator<String> it = tableOffsets.iterator();
		while (it.hasNext()) {
			def.append(it.next());
		}
		def.append("\t\tdefault:\n\t\t\treturn 0;\n");
		def.append("\t}\n}\n\n");

		setDefinition(def.toString());

		return def.toString();
	}
}
