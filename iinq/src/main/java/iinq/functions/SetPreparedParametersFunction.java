package iinq.functions;

public class SetPreparedParametersFunction extends IinqFunction {
	public SetPreparedParametersFunction() {
		super("iinq_set_param",
				"void iinq_set_param(iinq_prepared_sql *p, iinq_field_num_t field_num, ion_value_t val);\n",
				"void iinq_set_param(iinq_prepared_sql *p, iinq_field_num_t field_num, ion_value_t val) {\n" +
						"\tiinq_null_indicator_t *null_indicators = p->value;\n" +
						"\tunsigned char\t*data\t\t= ((char *) p->value) + iinq_calculate_offset(p->table, field_num);\n" +
						"\tif (NULL == val) {" +
						"\t\tiinq_set_null_indicator(null_indicators, field_num);\n" +
						"\t} else {\n" +
						"\t\tiinq_clear_null_indicator(null_indicators, field_num);\n" +
						"\t\tiinq_field_t type\t\t= iinq_get_field_type(p->table, field_num);\n" +
						"\t\tif (type == iinq_int) {\n" +
						"\t\t\tif (iinq_is_key_field(p->table, field_num))\n" +
						"\t\t\t\t*(int *) (p->key+iinq_calculate_key_offset(p->table, field_num)) = NEUTRALIZE(val, int);\n" +
						"\t\t\t*(int *) data = NEUTRALIZE(val,int);\n\t}\n" +
						"\t\telse {\n" +
						"\t\t\tsize_t size = iinq_calculate_offset(p->table, field_num+1)-iinq_calculate_offset(p->table, field_num);\n" +
						"\t\t\tif (iinq_is_key_field(p->table, field_num))\n" +
						"\t\t\t\tstrncpy(p->key+iinq_calculate_key_offset(p->table, field_num), val, size);\n" +
						"\t\t\tstrncpy(data, val, size);\n" +
						"\t\t}\n" +
						"\t}\n" +
						"}\n\n");
	}
}
