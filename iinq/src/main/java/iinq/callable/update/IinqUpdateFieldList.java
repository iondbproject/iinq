package iinq.callable.update;

import java.util.ArrayList;
import java.util.Iterator;

public class IinqUpdateFieldList {
	private ArrayList<UpdateField> fields;

	public IinqUpdateFieldList() {
		fields = new ArrayList<>();
	}

	public IinqUpdateFieldList(ArrayList<UpdateField> fields) {
		this.fields = fields;
	}

	public void addField(UpdateField field) {
		fields.add(field);
	}

	public String generateUpdateList() {
		StringBuilder list = new StringBuilder();
		Iterator<UpdateField> it = fields.iterator();
		list.append("IINQ_UPDATE_LIST(");

		while (it.hasNext()) {
			list.append(it.next().generateUpdate());
			list.append(", ");
		}

		list.setLength(list.length()-2);
		list.append(")");

		return list.toString();
	}


}
