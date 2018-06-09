package iinq.functions;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.metadata.IinqTable;
import unity.annotation.GlobalSchema;

import static iinq.functions.SchemaKeyword.*;

public class PrintFunction extends IinqFunction {
	public PrintFunction(String table_name, GlobalSchema metadata) {
		super("print_table_" + table_name,
				"void print_table_" + table_name + "(ion_dictionary_t * dictionary);\n", null);

	}

	public PrintFunction(IinqTable table) throws InvalidArgumentException {
		super("print_table_" + table.getTableName(),
				"void print_table_" + table.getTableName() + "(ion_dictionary_t * dictionary);\n",
				null);
		buildDefinition(table);
	}

	public String buildDefinition(IinqTable table) throws InvalidArgumentException {
		StringBuilder def = new StringBuilder();
		def.append("void print_table_" + table.getTableName() + "(ion_dictionary_t *dictionary) {\n");
		def.append("\n\tion_predicate_t predicate;\n");
		def.append("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");
		def.append("\tion_dict_cursor_t *cursor = NULL;\n");
		def.append("\tdictionary_find(dictionary, &predicate, &cursor);\n\n");
		def.append("\tion_record_t ion_record;\n");
		def.append("\tion_record.key		= malloc(" + table.getSchemaValue(PRIMARY_KEY_SIZE) + ");\n");
		def.append("\tion_record.value	= malloc(" + table.getSchemaValue(VALUE_SIZE) + ");\n\n");
		def.append("\tprintf(\"Table: " + table.getTableName() + "\\" + "n" + "\");\n");

		for (int j = 1, n = table.getNumFields(); j <= n; j++) {
			def.append("\tprintf(\"" + table.getFieldName(j) + "\t\");\n");
		}

		def.append("\tprintf(\"" + "\\" + "n" + "***************************************" + "\\" + "n" + "\");\n\n");

		def.append("\tion_cursor_status_t cursor_status;\n");
		def.append("\tunsigned char *value;\n\n");

		def.append("\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || " +
				"cursor_status == cs_cursor_initialized) {\n");
		def.append("\t\tvalue = ion_record.value;\n\n");

		String data_type;

		/* Print out columns */
		for (int j = 1, n = table.getNumFields(); j <= n; j++) {
			data_type = table.getIonFieldSize(j);

			if (data_type.contains("char")) {
				def.append("\n\t\tprintf(\"%s\t\", (char *) value);\n");

				if (j < (table.getNumFields())) {
					def.append("\t\tvalue += " + data_type + ";\n");
				}
			}

			/* Implement for all data types - for now assume int if not char or varchar */
			else {
				def.append("\n\t\tprintf(\"%i\t\", NEUTRALIZE(value, int));\n");

				if (j < (table.getNumFields())) {
					def.append("\t\tvalue += " + data_type + ";\n");
				}
			}
		}

		def.append("\n\t\tprintf(\"" + "\\" + "n" + "\");");
		def.append("\n\t}\n");
		def.append("\n\tprintf(\"" + "\\" + "n" + "\");\n\n");

		def.append("\tcursor->destroy(&cursor);\n");
		def.append("\tfree(ion_record.key);\n");
		def.append("\tfree(ion_record.value);\n}\n\n");

		setDefinition(def.toString());

		return def.toString();
	}
}
