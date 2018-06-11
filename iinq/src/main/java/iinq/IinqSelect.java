/******************************************************************************/
/**
 @file		    IinqSelect.java
 @author		Dana Klamut
 @copyright	    Copyright 2018
 The University of British Columbia,
 IonDB Project Contributors (see AUTHORS.md)
 @par Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:

 @par 1.Redistributions of source code must retain the above copyright notice,
 this list of conditions and the following disclaimer.

 @par 2.Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.

 @par 3.Neither the table_id of the copyright holder nor the names of its contributors
 may be used to endorse or promote products derived from this software without
 specific prior written permission.

 @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

package iinq;

import java.util.ArrayList;

public class IinqSelect {
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

    public IinqSelect(String name, int id, int num_w, int num_f, ArrayList<Integer> cols, ArrayList<String> ops,
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


