package iinq.functions.SelectFunctions;

import iinq.functions.IinqFunction;

public class GetIntFunction extends IinqFunction {
	public GetIntFunction() {
		super("getInt",
				"int getInt(iinq_result_set *select, int field_num);\n",
				"int getInt(iinq_result_set *select, int field_num) {\n" +
						"\tint i, count = 0;\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\terror              = iinq_open_source(\"SEL.inq\", &dictionary, &handler);\n\n" +
						"\tif (err_ok != error) {\n" +
						"\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n" +
						"\t}\n\n" +
						"\tdictionary_get(&dictionary, select->count, select->value);\n\n" +
						"\terror = ion_close_dictionary(&dictionary);\n\n" +
						"\tif (err_ok != error) {\n" +
						"\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n" +
						"\t}\n\n" +
						"\tfor (i = 0; i < *(int *) select->num_fields; i++) {\n" +
						"\t\tint field = *(int *) (select->fields + sizeof(int)*i);\n\n" +
						"\t\tif (getFieldType(select->table_id, field) == iinq_int) {\n" +
						"\t\t\tcount++;\n\t\t}\n\n" +
						"\t\tif (count == field_num) {\n" +
						"\t\t\treturn NEUTRALIZE(select->value + calculateOffset(select->table_id, field-1), int);\n" +
						"\t\t}\n\t}\n\n\treturn 0;\n}\n\n");
	}
}
