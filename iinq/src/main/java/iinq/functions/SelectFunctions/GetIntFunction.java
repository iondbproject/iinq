package iinq.functions.SelectFunctions;

import iinq.functions.CommonCode;
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
						"\tselect->status.error              = iinq_open_source(255, &dictionary, &handler);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +
						"\tdictionary_get(&dictionary, select->count, select->value);\n\n" +
						"\tselect->status.error = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +
						"\tfor (i = 0; i < *(int *) select->num_fields; i++) {\n" +
						"\t\tint field = *(int *) (select->fields + sizeof(int)*i);\n\n" +
						"\t\tif (getFieldType(select->table_id, field) == iinq_int) {\n" +
						"\t\t\tcount++;\n\t\t}\n\n" +
						"\t\tif (count == field_num) {\n" +
						"\t\t\treturn NEUTRALIZE(select->value + calculateOffset(select->table_id, field-1), int);\n" +
						"\t\t}\n\t}\n\n\treturn 0;\n}\n\n");
	}
}
