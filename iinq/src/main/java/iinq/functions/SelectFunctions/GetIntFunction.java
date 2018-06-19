package iinq.functions.SelectFunctions;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class GetIntFunction extends IinqFunction {
	public GetIntFunction() {
		super("getInt",
				"int getInt(iinq_result_set *select, int field_num);\n",
				"int getInt(iinq_result_set *select, int field_num) {\n" +
						"\tiinq_field_num_t i;\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\tselect->status.error = ion_init_master_table();\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +
						"\tselect->status.error              = ion_open_dictionary(&handler, &dictionary, select->id);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +
						"\tselect->status.error = ion_close_master_table();\n" +
						"\tdictionary_get(&dictionary, IONIZE(select->status.count,int), select->value);\n\n" +
						"\tselect->status.error = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +
						"\treturn NEUTRALIZE(select->value + calculateOffset(select->table_id, select->fields[field_num-1]), int);\n}\n\n");
						/*"\tfor (i = 0; i < select->num_fields; i++) {\n" +
						"\t\tiinq_field_num_t field = select->fields[i];\n\n" +
						"\t\tif (getFieldType(select->table_id, field) == iinq_int) {\n" +
						"\t\t\tcount++;\n\t\t}\n\n" +
						"\t\tif (count == field_num) {\n" +
						"\t\t\treturn NEUTRALIZE(select->value + calculateOffset(select->table_id, field), int);\n" +
						"\t\t}\n\t}\n\n\treturn 0;\n}\n\n");*/
	}
}
