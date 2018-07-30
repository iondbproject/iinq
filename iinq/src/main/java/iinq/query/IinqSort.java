package iinq.query;

import java.util.ArrayList;
import java.util.Iterator;

public class IinqSort {
	protected int limit;
	protected ArrayList<IinqSortElement> sortElements = new ArrayList<>();

	public enum IINQ_DIRECTION {
		IINQ_ASC,
		IINQ_DESC
	}

	class IinqSortElement {
		private IINQ_DIRECTION direction;
		private int tableId;
		private int fieldNum;
		private int dataType;

		IinqSortElement(IINQ_DIRECTION direction, int tableId, int fieldNum, int dataType) {
			this.direction = direction;
			this.tableId = tableId;
			this.fieldNum = fieldNum;
			this.dataType = dataType;
		}
	}

	public IinqSort(int limit) {
		this.limit = limit;
	}

	public void addSortElement(IINQ_DIRECTION direction, int tableId, int fieldNum, int dataType) {
		sortElements.add(new IinqSortElement(direction, tableId, fieldNum, dataType));
	}

	public int getNumSortElements() {
		return sortElements.size();
	}

	public String generateSortArray() {
		StringBuilder sortArray = new StringBuilder();
		sortArray.append("IINQ_ORDER_BY_LIST(");

		Iterator<IinqSortElement> it = sortElements.iterator();
		while(it.hasNext()) {
			IinqSortElement element = it.next();
			sortArray.append(String.format("IINQ_ORDER_BY(%d, %s), ", element.fieldNum, element.direction));
		}

		sortArray.setLength(sortArray.length()-2);
		sortArray.append(")");

		return sortArray.toString();
	}
}
