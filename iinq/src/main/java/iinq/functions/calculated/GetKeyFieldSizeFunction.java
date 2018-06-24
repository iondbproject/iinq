package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GetKeyFieldSizeFunction extends IinqFunction implements CalculatedFunction {
	HashMap<Integer, String> keyFieldCases = new HashMap<>();

	public GetKeyFieldSizeFunction() {
		super ("iinq_get_key_field_size",
				"size_t iinq_get_key_field_size(iinq_table_id table_id, iinq_field_num_t field_num);\n",
				null);
	}

	public void addTable(IinqTable table) {
		StringBuilder keyFieldCase = new StringBuilder();
		keyFieldCase.append("\t\t\tswitch (field_num) {\n");
		ArrayList<Integer> indices = table.getPrimaryKeyIndices();
		ArrayList<String> sizes = table.getIonKeyFieldSizes();
		Iterator<Integer> indicesIt = indices.iterator();
		Iterator<String> sizesIt = sizes.iterator();
		while (indicesIt.hasNext()) {
			keyFieldCase.append(String.format("\t\t\t\tcase %d:\n\t\t\t\t\treturn %s;\n", indicesIt.next(), sizesIt.next()));
		}
		keyFieldCase.append("\t\t\t}\n");
		keyFieldCases.put(table.getTableId(), keyFieldCase.toString());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();
		def.append("size_t iinq_get_key_field_size(iinq_table_id table_id, iinq_field_num_t field_num) {\n");
		def.append("\tswitch (table_id) {\n");
		for (Map.Entry<Integer, String> entry : keyFieldCases.entrySet()) {
			def.append(String.format("\t\tcase %d:\n", entry.getKey()));
			def.append(entry.getValue());
		}

		def.append("\t\tdefault:\n\t\t\treturn boolean_false;\n\t}\n}\n\n");
		setDefinition(def.toString());
		return def.toString();
	}
}
