package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GetKeyFieldOffsetFunction extends IinqFunction implements CalculatedFunction {
	HashMap<Integer, String> tableOffsetCases = new HashMap<>();

	public GetKeyFieldOffsetFunction() {
		super("iinq_calculate_key_offset",
				"size_t iinq_calculate_key_offset(iinq_table_id_t table_id, iinq_field_num_t field_num);\n",
				null);
	}

	public void addTable(IinqTable table) {
		StringBuilder offsetCase = new StringBuilder();
		StringBuilder offset = new StringBuilder();
		offset.append("0");
		ArrayList<Integer> indices = table.getPrimaryKeyIndices();
		ArrayList<String> sizes = table.getIonKeyFieldSizes();
		offsetCase.append("\t\t\tswitch (field_num) {\n");
		Iterator<Integer> indicesIt = indices.iterator();
		Iterator<String> sizesIt = sizes.iterator();
		while (indicesIt.hasNext()) {
			offsetCase.append(String.format("\t\t\t\tcase %d:\n" +
					"\t\t\t\t\treturn %s;\n", indicesIt.next(), offset.toString()));
			offset.append(" + ");
			offset.append(sizesIt.next());
		}
		offsetCase.append("\t\t\t}\n");
		tableOffsetCases.put(table.getTableId(), offsetCase.toString());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();
		def.append("size_t iinq_calculate_key_offset(iinq_table_id_t table_id, iinq_field_num_t field_num) {\n" +
				"\tswitch (table_id) {\n");
		for (Map.Entry<Integer, String> entry : tableOffsetCases.entrySet()) {
			def.append(String.format("\t\tcase %d:\n", entry.getKey()));
			def.append(entry.getValue());
		}
		def.append("\t}\n}\n");
		setDefinition(def.toString());
		return def.toString();
	}
}
