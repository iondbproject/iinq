package iinq;

/**
 * Created by danaklamut on 2018-02-12.
 */
public class insert {
    public String name;
    public String[] fields;
    public int count;
    public String sql;
    public int pos;
    public String value;
    public boolean[] prep_fields;
    public String key_type;
    public String key_field_num;
    public int id;

    public insert(
        String table_name,
        String[] field_array,
        int num_count,
        String sql_statement,
        int pos_val,
        String value_insert,
        boolean[] prep_fields_array,
        String table_key_type,
        String table_key_field_num,
        int table_id
    ) {
        name = table_name;
        fields = field_array;
        count = num_count;
        sql = sql_statement;
        pos = pos_val;
        value = value_insert;
        prep_fields = prep_fields_array;
        key_type = table_key_type;
        key_field_num = table_key_field_num;
        id = table_id;
    }
}
