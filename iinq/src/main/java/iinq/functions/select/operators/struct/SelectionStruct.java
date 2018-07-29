package iinq.functions.select.operators.struct;

import java.util.Arrays;
import java.util.LinkedHashSet;

public class SelectionStruct extends OperatorStruct {
	public SelectionStruct() {
		super("iinq_selection",
				new LinkedHashSet<>(Arrays.asList(
						new StructMember("num_conditions", "unsigned int"),
						new StructMember("conditions", "iinq_where_params_t *")
				)));
	}
}
