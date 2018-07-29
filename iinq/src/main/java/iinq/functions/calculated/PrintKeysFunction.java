package iinq.functions.calculated;

import iinq.functions.IinqFunction;
import iinq.metadata.IinqTable;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PrintKeysFunction extends IinqFunction implements CalculatedFunction {
	private HashMap<Integer, String> tableKeyPrints = new HashMap<>();

	public PrintKeysFunction() {
		super("iinq_print_keys",
				"ion_err_t iinq_print_keys(iinq_table_id_t table_id);\n",
				null);
	}

	public void addTable(IinqTable table) {
		StringBuilder keyPrint = new StringBuilder();
		ArrayList<Integer> indices = table.getPrimaryKeyIndices();
		ArrayList<String> sizes = table.getIonKeyFieldSizes();

		keyPrint.append(String.format("\t\t\tion_record.key = malloc(%s);\n", table.generateIonKeySize()));
		keyPrint.append("\t\t\tif (NULL == ion_record.key) {\n" +
				"\t\t\t\terror = err_out_of_memory;\n" +
				"\t\t\t\tgoto END;\n" +
				"\t\t\t}\n" +
				"\n");

		keyPrint.append(String.format("\t\t\tion_record.value = malloc(%s);\n", table.generateIonValueSize()));
		keyPrint.append("\t\t\tif (NULL == ion_record.value) {\n" +
				"\t\t\t\terror = err_out_of_memory;\n" +
				"\t\t\t\tgoto END;\n" +
				"\t\t\t}\n" +
				"\n");

		keyPrint.append("\t\t\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || cursor_status == cs_cursor_initialized) {\n" +
				"\t\t\t\tkey = ion_record.key;\n\n");

		Iterator<Integer> it = indices.iterator();
		int index;
		while (it.hasNext()) {
			index = it.next();
			String fieldSize = table.getIonFieldSize(index);
			switch (table.getFieldType(index)) {
				case Types.INTEGER:
					if (it.hasNext())
						keyPrint.append(String.format("\t\t\t\tprintf(\"%%%dd, \", %s);\n", 10, "NEUTRALIZE(key, int)"));
					else
						keyPrint.append(String.format("\t\t\t\tprintf(\"%%%dd\\n\", %s);\n", 10, "NEUTRALIZE(key, int)"));
					break;
				case Types.CHAR:
				case Types.VARCHAR:
					if (it.hasNext())
						keyPrint.append(String.format("\t\t\t\tprintf(\"%%%ds, \", %s);\n", table.getFieldSize(index), "(char*) key"));
					else
						keyPrint.append(String.format("\t\t\t\tprintf(\"%%%ds\\n\", %s);\n", table.getFieldSize(index), "(char*) key"));
					break;
			}
			if (it.hasNext())
				keyPrint.append(String.format("\t\t\t\tkey += %s;\n\n", fieldSize));
		}

		keyPrint.append("\t\t\t}\n" +
				"\t\t\tbreak;\n");

		tableKeyPrints.put(table.getTableId(), keyPrint.toString());
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();
		def.append("ion_err_t iinq_print_keys(iinq_table_id_t table_id) {\n" +
				"\tion_dictionary_t 			dictionary;\n" +
				"\tion_dictionary_handler_t		handler;\n" +
				"\tion_err_t					error;\n" +
				"\tion_predicate_t				predicate;\n" +
				"\tion_dict_cursor_t			*cursor = NULL;\n" +
				"\tion_cursor_status_t			cursor_status;\n" +
				"\tion_record_t ion_record;\n" +
				"\tion_record.key = NULL;\n" +
				"\tion_record.value = NULL;\n" +
				"\n" +
				"\terror = iinq_open_source(table_id, &dictionary, &handler);\n" +
				"\tif (err_ok != error) {\n" +
				"\t\treturn error;\n" +
				"\t}\n" +
				"\n" +
				"\terror = dictionary_build_predicate(&predicate, predicate_all_records);\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tion_close_dictionary(&dictionary);\n" +
				"\t\treturn error;\n" +
				"\t}\n" +
				"\n" +
				"\terror = dictionary_find(&dictionary, &predicate, &cursor);\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tion_close_dictionary(&dictionary);\n" +
				"\t\treturn error;\n" +
				"\t}\n" +
				"\n" +
				"\tunsigned char *key;" +
				"\tswitch (table_id) {\n");

		for (Map.Entry<Integer, String> entry : tableKeyPrints.entrySet()) {
			def.append(String.format("\t\tcase %d:\n", entry.getKey()));
			def.append(entry.getValue());
		}

		def.append("\t}\n" +
				"\tprintf(\"\\n\");\n" +
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
