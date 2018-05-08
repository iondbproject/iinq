package iinq;

public class IinqField {
	private String fieldName;
	private int fieldType;
	private String fieldTypeName;
	private Integer fieldSize;

	public IinqField(String fieldName, int fieldType, String fieldTypeName, int fieldSize) {
		this.fieldName = fieldName;
		this.fieldType = fieldType;
		this.fieldTypeName = fieldTypeName;
		this.fieldSize = fieldSize;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public int getFieldType() {
		return fieldType;
	}

	public void setFieldType(int fieldType) {
		this.fieldType = fieldType;
	}

	public String getFieldTypeName() {
		return fieldTypeName;
	}

	public void setFieldTypeName(String fieldTypeName) {
		this.fieldTypeName = fieldTypeName;
	}

	public int getFieldSize() {
		return fieldSize;
	}

	public void setFieldSize(Integer fieldSize) {
		this.fieldSize = fieldSize;
	}
}