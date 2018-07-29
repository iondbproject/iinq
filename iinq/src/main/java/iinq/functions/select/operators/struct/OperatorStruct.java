package iinq.functions.select.operators.struct;

import java.util.LinkedHashSet;

public abstract class OperatorStruct {
	protected String name;
	protected LinkedHashSet<StructMember> members;

	public OperatorStruct(String name, LinkedHashSet<StructMember> members) {
		this.name = name;
		this.members = new LinkedHashSet<>();
		this.members.add(new StructMember("super", "iinq_query_operator_parent_t"));
		this.members.addAll(members);
	}

	public String generateStructDefinition() {
		StringBuilder struct = new StringBuilder();

		struct.append(String.format("typedef struct %s %s;\n\n", name, name + "_t"));
		struct.append(String.format("struct %s {\n", name));

		for (StructMember member : members) {
			struct.append(String.format("\t%s %s;\n", member.type, member.name));
		}

		struct.append("};\n\n");

		return struct.toString();
	}

	public String getName() {
		return name;
	}
}
