package iinq;

import java.util.ArrayList;

/**
 * Created by danaklamut on 2018-03-08.
 */
public class delete_fields {
    public String table_name;
    public int table_id;
    public int num_wheres;
    public ArrayList<Integer> fields;
    public ArrayList<String> operators;
    public ArrayList<String> values;
    public ArrayList<String> field_types;
    public String key_size;
    public String value_size;

    public delete_fields(String name, int id, int num, ArrayList<Integer> cols, ArrayList<String> ops, ArrayList<String> vals, ArrayList<String> types, String size_k, String size_v) {
        table_name = name;
        table_id = id;
        num_wheres = num;
        fields = cols;
        operators = ops;
        values = vals;
        field_types = types;
        key_size = size_k;
        value_size = size_v;
    }
}
