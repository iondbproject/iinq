package iinq.functions.select.operators.destroy;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class DictionaryDestroyFunction extends OperatorDestroyFunction {
	public DictionaryDestroyFunction() {
		super("iinq_dictionary_operator_destroy",
				"void iinq_dictionary_operator_destroy(iinq_query_operator_t **query_operator);\n",
				"void iinq_dictionary_operator_destroy(iinq_query_operator_t **query_operator) {\n" +
						"\tif (NULL != *query_operator) {\n" +
						"\t\tif (NULL != (*query_operator)->instance){\n" +
						"\t\t\tiinq_dictionary_operator_t *dict_op = (iinq_dictionary_operator_t *) (*query_operator)->instance;\n" +
						"\n" +
						"\t\t\tif (NULL != dict_op->super.field_info) {\n" +
						"\t\t\t\tfree(dict_op->super.field_info);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != dict_op->super.fields) {\n" +
						"\t\t\t\tfree(dict_op->super.fields);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != dict_op->record.value) {\n" +
						"\t\t\t\tfree(dict_op->record.value);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != dict_op->record.key) {\n" +
						"\t\t\t\tfree(dict_op->record.key);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != dict_op->cursor) {\n" +
						"\t\t\t\tdict_op->cursor->destroy(&dict_op->cursor);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tion_close_dictionary(&dict_op->dictionary);\n" +
						"\n" +
						"\t\t\tfree(dict_op);\n" +
						"\t\t}\n" +
						"\t\tfree(*query_operator);\n" +
						"\t\t*query_operator = NULL;\n" +
						"\t}" +
						"}\n\n"

		);
	}
}
