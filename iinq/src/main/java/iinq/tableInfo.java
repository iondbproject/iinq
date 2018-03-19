package iinq;

import java.util.ArrayList;

/**
 * Created by danaklamut on 2018-03-08.
 */
public class tableInfo {
    public int tableId;
    public int numFields;
    public ArrayList<String> field_types;
    public ArrayList<String> field_sizes;

    public tableInfo(int id, int num, ArrayList<String> types, ArrayList<String> sizes) {
        tableId     = id;
        numFields   = num;
        field_types = types;
        field_sizes = sizes;
    }
}
