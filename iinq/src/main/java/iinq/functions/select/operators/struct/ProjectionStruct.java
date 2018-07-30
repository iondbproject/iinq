package iinq.functions.select.operators.struct;

import java.util.Arrays;
import java.util.LinkedHashSet;

public class ProjectionStruct extends OperatorStruct {
	public ProjectionStruct() {
		super("iinq_projection",
				new LinkedHashSet<>(Arrays.asList(
						new StructMember("input_field_nums", "iinq_field_num_t *")
		)));
	}
}
