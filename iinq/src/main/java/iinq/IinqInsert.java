/******************************************************************************/
/**
 @file		    IinqInsert.java
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

public class IinqInsert {
    public int table_id;
    public String[] fields;
    public IinqInsertFields insertFields;
    public ArrayList<Integer> int_fields;
    public ArrayList<Integer> string_fields;
    public int count;
    public String sql;
    public boolean[] prep_fields;
    public String key_type;
    public String key_field_num;

    public IinqInsert(
        int table_id,
        String[] field_array,
        IinqInsertFields insert_fields,
        ArrayList<Integer> int_cols,
        ArrayList<Integer> string_cols,
        int num_count,
        boolean[] prep_fields_array,
        String table_key_type,
        String table_key_field_num
    ) {
        this.table_id = table_id;
        fields = field_array;
        insertFields = insert_fields;
        int_fields = int_cols;
        string_fields = string_cols;
        count = num_count;
        prep_fields = prep_fields_array;
        key_type = table_key_type;
        key_field_num = table_key_field_num;
    }
}
