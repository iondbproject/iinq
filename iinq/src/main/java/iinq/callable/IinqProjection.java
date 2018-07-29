/******************************************************************************/
/**
 @file		    IinqProjection.java
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

package iinq.callable;

import iinq.functions.select.operators.ProjectionOperator;

import java.util.ArrayList;
import java.util.Iterator;

public class IinqProjection implements Callable{
    protected ArrayList<Integer> fieldList;
    protected ProjectionOperator operator;

    public IinqProjection(ArrayList<Integer> fieldList) {
        this.fieldList = fieldList;
    }

    public void setOperator(ProjectionOperator operator) {
        this.operator = operator;
    }

    public int getNumFields() {
        return fieldList.size();
    }

    public ArrayList<Integer> getFieldList() {
        return fieldList;
    }

    public String getIinqProjectionList() {
        StringBuilder list = new StringBuilder();
        Iterator<Integer> it = fieldList.iterator();

        list.append("IINQ_PROJECTION_LIST(");

        while (it.hasNext()) {
            list.append(it.next());
            list.append(", ");
        }

        list.setLength(list.length()-2);
        list.append(")");

        return list.toString();
    }

    public void setFieldList(ArrayList<Integer> fieldList) {
        this.fieldList = fieldList;
    }

    public String generateFunctionCall() {
        return String.format("%s(%s, %d, %s)", operator.generateInitFunctionCall(), operator.getInputOperators().get(0).generateInitFunctionCall(), getNumFields(), getIinqProjectionList());
    }
}


