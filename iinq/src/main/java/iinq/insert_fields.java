package iinq;

import java.util.ArrayList;

/**
 * Created by danaklamut on 2018-02-24.
 */
public class insert_fields {
    public String table;
    public ArrayList<String> fields;
    public ArrayList<String> field_types;

    public insert_fields(String name, ArrayList<String> vals, ArrayList<String> types) {
        table = name;
        fields = vals;
        field_types = types;
    }
}
