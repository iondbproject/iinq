package iinq.query;

import java.util.ArrayList;

public class IinqSort {
	private int limit;
	private ArrayList<IinqSortElement> sortElements = new ArrayList<>();

	public enum DIRECTION {
		ASC,
		DESC
	}

	class IinqSortElement {
		private DIRECTION direction;
		private int tableId;
		private int fieldNum;
		private int dataType;

		IinqSortElement(DIRECTION direction, int tableId, int fieldNum, int dataType) {
			this.direction = direction;
			this.tableId = tableId;
			this.fieldNum = fieldNum;
			this.dataType = dataType;
		}
	}

	public IinqSort(int limit) {
		this.limit = limit;
	}

	public void addSortElement(DIRECTION direction, int tableId, int fieldNum, int dataType) {
		sortElements.add(new IinqSortElement(direction, tableId, fieldNum, dataType));
	}
}
