package iinq.functions.SelectFunctions;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class GetStringFunction extends IinqFunction {
	public GetStringFunction() {
		super("getString",
				"char* getString(iinq_result_set *select, int field_num);\n",
				"char* getString(iinq_result_set *select, int field_num) {\n" +
						"\tiinq_field_num_t i, count = 0;\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\tselect->status.error = ion_init_master_table();\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +
						"\tselect->status.error              = ion_open_dictionary(&handler, &dictionary, select->id);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +
						"\tselect->status.error = ion_close_master_table();\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +
						"\tdictionary_get(&dictionary, select->status.count, select->value);\n\n" +
						"\terror = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +
						"\tfor (i = 0; i < select->num_fields; i++) {\n" +
						"\t\tint field = *(int *) (select->fields + sizeof(int)*i);\n\n" +
						"\t\tif (getFieldType(select->table_id, field) == iinq_null_terminated_string) {\n" +
						"\t\t\tcount++;\n\t\t}\n\n" +
						"\t\tif (count == field_num) {\n" +
						"\t\t\treturn (char *) (select->value + calculateOffset(select->table_id, field-1));\n" +
						"\t\t}\n\t}\n\n\treturn \"\";\n}\n\n");
	}
}
