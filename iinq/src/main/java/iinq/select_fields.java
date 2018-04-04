package iinq;

import java.util.ArrayList;

/**
 * Created by danaklamut on 2018-03-18.
 */
public class select_fields {
    public String table_name;
    public int table_id;
    public int num_wheres;
    public int num_fields;
    public ArrayList<Integer> where_fields;
    public ArrayList<String> where_operators;
    public ArrayList<String> where_values;
    public ArrayList<String> where_field_types;
    public String key_type;
    public String key_size;
    public String value_size;
    public ArrayList<Integer> fields;
    public String return_value;

    public select_fields(String name, int id, int num_w, int num_f, ArrayList<Integer> cols, ArrayList<String> ops,
                         ArrayList<String> vals, ArrayList<String> types_w, String type_k, String size_k, String size_v, ArrayList<Integer> field_list,
                         String val) {
        table_name = name;
        table_id = id;
        num_wheres = num_w;
        num_fields = num_f;
        where_fields = cols;
        where_operators = ops;
        where_values = vals;
        where_field_types = types_w;
        key_type = type_k;
        key_size = size_k;
        value_size = size_v;
        fields = field_list;
        return_value = val;
    }
}


