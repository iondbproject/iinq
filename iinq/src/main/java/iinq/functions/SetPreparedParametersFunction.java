package iinq.functions;

public class SetPreparedParametersFunction extends IinqFunction {
	public SetPreparedParametersFunction() {
		super("setParam",
				"void setParam(iinq_prepared_sql *p, iinq_field_num_t field_num, ion_value_t val);\n",
				"void setParam(iinq_prepared_sql *p, iinq_field_num_t field_num, ion_value_t val) {\n" +
						"\tunsigned char\t*data\t\t= p->value;\n\n" +
						"\tiinq_field_t type\t\t= getFieldType(p->table, field_num);\n" +
						"\tdata += calculateOffset(p->table, field_num);\n\n" +
						"\tif (type == iinq_int) {\n" +
						"\t\tif (iinq_is_key_field(p->table, field_num))\n" +
						"\t\t\t*(int *) (p->key+iinq_calculate_key_offset(p->table, field_num)) = NEUTRALIZE(val,int);\n" +
						"\t\t*(int *) data = NEUTRALIZE(val,int);\n\t}\n" +
						"\telse {\n" +
						"\t\tsize_t size = calculateOffset(p->table, field_num+1)-calculateOffset(p->table, field_num);\n" +
						"\t\tif (iinq_is_key_field(p->table, field_num))\n" +
						"\t\t\tstrncpy(p->key+iinq_calculate_key_offset(p->table, field_num), val, size);\n" +
						"\t\tstrncpy(data, val, size);\n\t}\n}\n\n");
	}
}
