package iinq;

public class InsertFieldElement implements Comparable<InsertFieldElement> {
	public int fieldNum;
	public String value;

	public InsertFieldElement(int fieldNum, String value) {
		this.fieldNum = fieldNum;
		this.value = value;
	}

	public int compareTo(InsertFieldElement o) {
		return fieldNum - o.fieldNum;
	}
}
