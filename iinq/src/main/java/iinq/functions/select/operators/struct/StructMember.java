package iinq.functions.select.operators.struct;

public class StructMember {
	String name;
	String type;

	public StructMember(String name, String type) {
		this.name = name;
		this.type = type;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}
}
