package iinq.callable.update;

public class UpdateField {
	private int fieldNum;
	ImplicitFieldInfo implicitFieldInfo;
	String value;

	public UpdateField(int fieldNum, ImplicitFieldInfo implicitFieldInfo, String value) {
		this.fieldNum = fieldNum;
		this.implicitFieldInfo = implicitFieldInfo;
		this.value = value;
	}

	public UpdateField(int fieldNum, String value) {
		this(fieldNum, null, value);
	}

	public UpdateField(int fieldNum, ImplicitFieldInfo implicitFieldInfo, int value) {
		this(fieldNum, implicitFieldInfo, "IONIZE(" + value + ", int)");
	}

	public UpdateField(int fieldNum, int value) {
		this(fieldNum, null, value);
	}

	public String generateUpdate() {
		StringBuilder update = new StringBuilder();
		update.append("IINQ_UPDATE(");
		update.append(fieldNum);
		update.append(", ");
		if (!isImplicit()) {
			update.append("0, 0, ");
		} else {
			update.append(implicitFieldInfo.implicitFieldNum);
			update.append(", ");
			update.append(implicitFieldInfo.operator);
			update.append(", ");
		}
		update.append(value);
		update.append(")");

		return update.toString();
	}

	public boolean isImplicit() {
		return implicitFieldInfo != null;
	}
}
