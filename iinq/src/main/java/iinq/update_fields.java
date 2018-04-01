package iinq;

import unity.predicates.In;

import java.util.ArrayList;

/**
 * Created by danaklamut on 2018-03-18.
 */
public class update_fields {
    public String table_name;
    public int table_id;
    public int num_wheres;
    public int num_updates;
    public ArrayList<Integer> where_fields;
    public ArrayList<String> where_operators;
    public ArrayList<String> where_values;
    public ArrayList<String> where_field_types;
    public String key_size;
    public String value_size;
    public String ion_key;
    public ArrayList<Integer> update_fields;
    public ArrayList<Boolean> implicit;
    public ArrayList<Integer> implicit_fields;
    public ArrayList<String> update_operators;
    public ArrayList<String> update_values;
    public ArrayList<String> update_field_types;

    public update_fields(String name, int id, int num_w, int num_u, ArrayList<Integer> cols, ArrayList<String> ops,
                         ArrayList<String> vals, ArrayList<String> types_w, String size_k, String size_v, String key_i,
                         ArrayList<Integer> fields_u, ArrayList<Boolean> bool_i, ArrayList<Integer> fields_i, ArrayList<String> ops_u,
                         ArrayList<String> vals_u, ArrayList<String> types_u) {
        table_name = name;
        table_id = id;
        num_wheres = num_w;
        num_updates = num_u;
        where_fields = cols;
        where_operators = ops;
        where_values = vals;
        where_field_types = types_w;
        key_size = size_k;
        value_size = size_v;
        ion_key = key_i;
        update_fields = fields_u;
        implicit = bool_i;
        implicit_fields = fields_i;
        update_operators = ops_u;
        update_values = vals_u;
        update_field_types = types_u;
    }
}

