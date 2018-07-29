package iinq.functions.select.operators.struct;

import java.util.Arrays;
import java.util.LinkedHashSet;

public class ProjectionStruct extends OperatorStruct {
	public ProjectionStruct() {
		super("iinq_projection",
				new LinkedHashSet<>(Arrays.asList(
						new StructMember("input_field_nums", "iinq_field_num_t *")
						//new StructMember("null_indictator", "iinq_null_indicator_t *")
/*						new StructMember("num_fields", "iinq_field_num_t"),
						new StructMember("table_info", "iinq_table_info_t *"),
						new StructMember("fields", "ion_value_t *")*/
		)));
	}
}
