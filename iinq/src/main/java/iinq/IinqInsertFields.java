/******************************************************************************/
/**
 @file		    IinqInsertFields.java
 @author		Dana Klamut, Kai Neubauer
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

 @par 3.Neither the name of the copyright holder nor the names of its contributors
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
import java.util.Collections;

public class IinqInsertFields {
	public ArrayList<FieldNode> fields;
	public int total_fields;
	class FieldNode implements Comparable<FieldNode> {
		int fieldNum;
		String value;

		@Override
		public int compareTo(FieldNode o) {
			return this.fieldNum -o.fieldNum;
		}

		FieldNode(int fieldNum, String value) {
			this.fieldNum = fieldNum;
			this.value = value;
		}
	}

    public IinqInsertFields(ArrayList<String> vals, int[] field_nums, int total_fields) {
        fields = new ArrayList<>();
        for (int i = 0; i < field_nums.length; i++) {
        	fields.add(new FieldNode(field_nums[i], vals.get(i)));
		}
		this.total_fields = total_fields;
    }

    public void sortFields() {
		Collections.sort(this.fields);
	}
}
