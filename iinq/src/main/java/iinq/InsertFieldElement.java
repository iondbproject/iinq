package iinq;

import unity.annotation.AnnotatedSourceField;

public class InsertFieldElement implements Comparable<InsertFieldElement> {
	int fieldNum;
	String value;

	public InsertFieldElement(int fieldNum, String value) {
		this.fieldNum = fieldNum;
		this.value = value;
	}

	public int compareTo(InsertFieldElement o) {
		return fieldNum - o.fieldNum;
	}
}
