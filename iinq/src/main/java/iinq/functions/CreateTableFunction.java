package iinq.functions;

import static iinq.functions.CommonCode.*;

public class CreateTableFunction extends IinqFunction {
	public CreateTableFunction() {
		super("create_table", "void create_table(iinq_table_id tableId, ion_key_type_t keyType, ion_key_size_t keySize, ion_value_size_t project_size);\n",
				"void create_table(iinq_table_id tableId, ion_key_type_t keyType, ion_key_size_t keySize, ion_value_size_t project_size) {\n" +
				"\tion_err_t error = iinq_create_source(tableId, keyType, keySize, project_size);\n" +
				error_check() + "}\n\n");
	}
}
