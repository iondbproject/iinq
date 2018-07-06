package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class CalculatedPrintFunction extends IinqFunction implements CalculatedFunction {
	private HashMap<Integer, PrintParams> tablePrintParams = new HashMap<>();

	class PrintParams {
		String keySize;
		String valueSize;
		String printCode;

		private PrintParams(String keySize, String valueSize, String printCode) {
			this.keySize = keySize;
			this.valueSize = valueSize;
			this.printCode = printCode;
		}

		PrintParams(IinqTable table) {
			this(table.generateIonKeySize(), table.generateIonValueSize(), buildPrintCode(table));
		}
	}

	public CalculatedPrintFunction() {
		super("print_table",
				"ion_err_t print_table(iinq_table_id tableId);\n",
				null);
	}

	private String buildPrintCode(IinqTable table) {
		StringBuilder printCode = new StringBuilder();
		printCode.append("\t\t\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || cursor_status == cs_cursor_initialized) {\n" +
				"\t\t\t\tvalue = ion_record.value;\n\n");
		for (int i = 1, n = table.getNumFields(); i <= n; i++) {
			String fieldSize = table.getIonFieldSize(i);
			switch (table.getFieldType(i)) {
				case Types.INTEGER:
					if (i < n)
						printCode.append(String.format("\t\t\t\tprintf(\"%%%dd, \", %s);\n", 10, "NEUTRALIZE(value, int)"));
					else
						printCode.append(String.format("\t\t\t\tprintf(\"%%%dd\\n\", %s);\n", 10, "NEUTRALIZE(value, int)"));
					break;
				case Types.CHAR:
				case Types.VARCHAR:
					if (i < n)
						printCode.append(String.format("\t\t\t\tprintf(\"%%%ds, \", %s);\n", table.getFieldSize(i), "(char*) value"));
					else
						printCode.append(String.format("\t\t\t\tprintf(\"%%%ds\\n\", %s);\n", table.getFieldSize(i), "(char*) value"));
					break;
			}
			if (i < n)
				printCode.append(String.format("\t\t\t\tvalue += %s;\n\n", fieldSize));
		}
		printCode.append("\t\t\t}\n");
		printCode.append("\t\t\tprintf(\"\\n\");\n");
		return printCode.toString();
	}

	public void addTable(IinqTable table) {
		tablePrintParams.put(table.getTableId(), new PrintParams(table));
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();
		def.append("ion_err_t print_table(iinq_table_id tableId) {\n" +
				"\tion_dictionary_t dictionary;\n" +
				"\tion_dictionary_handler_t handler;\n" +
				"\n" +
				"\tion_cursor_status_t cursor_status;\n" +
				"\n" +
				"\tion_record_t ion_record;\n" +
				"\tion_record.key = NULL;\n" +
				"\tion_record.value = NULL;\n" +
				"\tion_dict_cursor_t *cursor = NULL;\n" +
				"\n" +
				"\tdictionary.handler = &handler;\n" +
				"\n" +
				"\tion_err_t error = iinq_open_source(tableId, &dictionary, &handler);\n" +
				"\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tgoto END;\n" +
				"\t}\n" +
				"\n" +
				"\tion_predicate_t predicate;\n" +
				"\tdictionary_build_predicate(&predicate, predicate_all_records);\n" +
				"\n" +
				"\tdictionary_find(&dictionary, &predicate, &cursor);\n" +
				"\tswitch(tableId) {\n");
		for (Map.Entry<Integer, PrintParams> entry : tablePrintParams.entrySet()) {
			def.append(String.format("\t\tcase %d:\n",entry.getKey()));
			def.append(String.format("\t\t\tion_record.key = malloc(%s);\n",entry.getValue().keySize));
			def.append("\t\t\tif (NULL == ion_record.key) {\n" +
					"\t\t\t\terror = err_out_of_memory;\n" +
					"\t\t\t\tgoto END;\n" +
					"\t\t\t}\n");
			def.append(String.format("\t\t\tion_record.value = malloc(%s);\n",entry.getValue().valueSize));
			def.append("\t\t\tif (NULL == ion_record.value) {\n" +
					"\t\t\t\terror = err_out_of_memory;\n" +
					"\t\t\t\tgoto END;\n" +
					"\t\t\t}\n");
			def.append("\t\t\tbreak;\n");
		}
		def.append("\t}\n\n" +
				"\tunsigned char *value;\n" +
				"\tswitch (tableId) {\n");
		for (Map.Entry<Integer, PrintParams> entry : tablePrintParams.entrySet()) {
			def.append(String.format("\t\tcase %d:\n",entry.getKey()));
			def.append(entry.getValue().printCode);
			def.append("\t\t\tbreak;\n");
		}
		def.append("\t}\n" +
				"END:\n" +
				"\tif (NULL != cursor) {\n" +
				"\t\tcursor->destroy(&cursor);\n" +
				"\t}\n\n" +
				"\tif (NULL != ion_record.key) {\n" +
				"\t\tfree(ion_record.key);\n" +
				"\t};\n\n" +
				"\tif (NULL != ion_record.value) {\n" +
				"\t\tfree(ion_record.value);\n" +
				"\t};\n\n" +
				"\tion_close_dictionary(&dictionary);\n\n" +
				"\treturn error;\n" +
				"}\n" +
				"\n");
		setDefinition(def.toString());
		return def.toString();
	}

}
