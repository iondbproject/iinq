package iinq;

import java.util.ArrayList;

/**
 * Created by danaklamut on 2018-03-18.
 */
public class create_fields {
    public String table_name;
    public String key_type;
    public String key_size;
    public String value_size;

    public create_fields(String name, String type_k, String size_k, String size_v) {
        table_name = name;
        key_type = type_k;
        key_size = size_k;
        value_size = size_v;
    }
}


