package iinq.functions;

public class SetPreparedParametersFunction extends IinqFunction {
	public SetPreparedParametersFunction() {
		super("setParam",
				"void setParam(iinq_prepared_sql p, int field_num, void *val);\n",
				"void setParam(iinq_prepared_sql p, int field_num, void *val) {\n" +
						"\tunsigned char\t*data\t\t= p.value;\n\n" +
						"\tiinq_field_t type\t\t= getFieldType(*p.table, field_num);\n" +
						"\tdata += calculateOffset(*p.table, field_num);\n\n" +
						"\tif (type == iinq_int) {\n" +
						"\t\t*(int *) data = (int) val;\n\t}\n" +
						"\telse {\n" +
						"\t\tstrncpy(data, val, (calculateOffset(*p.table, field_num+1)-calculateOffset(*p.table, field_num)));\n\t}\n}\n\n");
	}
}
