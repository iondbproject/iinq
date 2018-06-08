package iinq.functions;

import iinq.metadata.IinqTable;

import java.util.ArrayList;
import java.util.Iterator;

public class GetFieldTypeFunction extends IinqFunction implements CalculatedFunction {
	private ArrayList<String> tableFieldTypes = new ArrayList<>();

	public GetFieldTypeFunction() {
		super(
				"getFieldType",
				"iinq_field_t getFieldType(const unsigned char *table, int field_num);\n",
				null
		);
	}

	public void addTable(IinqTable table) {
		StringBuilder fieldTypes = new StringBuilder();
		StringBuilder total = new StringBuilder();
		fieldTypes.append("\t\tcase " + table.getTableId() + " : {\n");
		fieldTypes.append("\t\t\tswitch (field_num) {\n");
		for (int i = 1, n = table.getNumFields(); i <= n; i++) {
			fieldTypes.append("\t\t\t\tcase " + i + " :\n");
			fieldTypes.append("\t\t\t\t\treturn ");
			total.append(table.getIonFieldType(i));
			fieldTypes.append(total.toString());
			fieldTypes.append(";\n");
			total.append(" + ");
		}
		fieldTypes.append("\t\t\t\tdefault:\n\t\t\t\t\treturn 0;\n");
		fieldTypes.append("\t\t\t}\n\t\t}\n");

		tableFieldTypes.add(fieldTypes.toString());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();
		def.append("iinq_field_t getFieldType(const unsigned char *table, int field_num) {\n\n");
		def.append("\tswitch (*(int *) table) {\n");
		Iterator<String> it = tableFieldTypes.iterator();
		while (it.hasNext()) {
			def.append(it.next());
		}
		def.append("\t\tdefault:\n\t\t\treturn 0;\n");
		def.append("\t}\n}\n\n");

		return def.toString();
	}
}
