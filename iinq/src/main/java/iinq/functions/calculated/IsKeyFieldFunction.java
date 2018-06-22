package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IsKeyFieldFunction extends IinqFunction implements CalculatedFunction {
	HashMap<Integer, ArrayList<Integer>> keyIndices = new HashMap<>();

	public IsKeyFieldFunction() {
		super("iinq_is_key_field",
				"ion_boolean_t iinq_is_key_field(iinq_table_id table_id, iinq_field_num_t field_num);\n",
				null);
	}

	public void addTable(IinqTable table) {
		keyIndices.put(table.getTableId(), table.getPrimaryKeyIndices());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();

		def.append("ion_boolean_t iinq_is_key_field(iinq_table_id table_id, iinq_field_num_t field_num) {\n" +
						"\tswitch (table_id) {\n");

		for (Map.Entry<Integer, ArrayList<Integer>> entry : keyIndices.entrySet()) {
			def.append(String.format("\t\tcase %d:\n\t\t\tswitch (field_num) {\n", entry.getKey()));
			Iterator<Integer> it = entry.getValue().iterator();

			while (it.hasNext()) {
				def.append(String.format("\t\t\t\tcase %d:\n", it.next()));
			}

			def.append("\t\t\t\t\treturn boolean_true;\n");
			def.append("\t\t\t\tdefault:\n\t\t\t\t\treturn boolean_false;\n\t\t\t}\n");
		}

		def.append("\t\tdefault:\n\t\t\treturn boolean_false;\n\t}\n}\n\n");

		setDefinition(def.toString());
		return def.toString();
	}
}
