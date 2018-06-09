/******************************************************************************/
/**
 @file		    IinqUpdate.java
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

public class IinqUpdate {
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
    public int implicit_count;

    public IinqUpdate(int id, int num_w, int num_u, ArrayList<Integer> cols, ArrayList<String> ops,
                      ArrayList<String> vals, ArrayList<String> types_w, String size_k, String size_v, String key_i,
                      ArrayList<Integer> fields_u, ArrayList<Boolean> bool_i, ArrayList<Integer> fields_i, ArrayList<String> ops_u,
                      ArrayList<String> vals_u, ArrayList<String> types_u, int i_count) {
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
        implicit_count = i_count;
    }
}

