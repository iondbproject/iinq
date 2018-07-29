package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.Iterator;

public class GetFieldTypeFunction extends IinqFunction implements CalculatedFunction {
	private ArrayList<String> tableFieldTypes = new ArrayList<>();

	public GetFieldTypeFunction() {
		super(
				"iinq_get_field_type",
				"iinq_field_t iinq_get_field_type(iinq_table_id_t tableId, iinq_field_num_t field_num);\n",
				null
		);
	}

	public void addTable(IinqTable table) {
		StringBuilder fieldTypes = new StringBuilder();
		fieldTypes.append("\t\tcase ").append(table.getTableId()).append(" : {\n");
		fieldTypes.append("\t\t\tswitch (field_num) {\n");
		for (int i = 1, n = table.getNumFields(); i <= n; i++) {
			fieldTypes.append("\t\t\t\tcase " + i + " :\n");
			fieldTypes.append("\t\t\t\t\treturn ");
			fieldTypes.append(table.getIonFieldType(i));
			fieldTypes.append(";\n");
		}
		fieldTypes.append("\t\t\t\tdefault:\n\t\t\t\t\treturn 0;\n");
		fieldTypes.append("\t\t\t}\n\t\t}\n");

		tableFieldTypes.add(fieldTypes.toString());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();
		def.append("iinq_field_t iinq_get_field_type(iinq_table_id_t table, iinq_field_num_t field_num) {\n\n");
		def.append("\tswitch (table) {\n");
		Iterator<String> it = tableFieldTypes.iterator();
		while (it.hasNext()) {
			def.append(it.next());
		}
		def.append("\t\tdefault:\n\t\t\treturn 0;\n");
		def.append("\t}\n}\n\n");

		setDefinition(def.toString());

		return def.toString();
	}
}
