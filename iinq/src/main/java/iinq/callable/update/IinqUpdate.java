/******************************************************************************/
/**
 @file		    IinqUpdate.java
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

package iinq.callable.update;

import iinq.IinqWhere;
import iinq.callable.Callable;

import java.util.ArrayList;

public class IinqUpdate implements Callable {
    public int tableId;
    public int numWheres;
    public int numUpdates;
    public IinqWhere where;
    public IinqUpdateFieldList updateFieldList;

    public IinqUpdate(int tableId, IinqWhere where, int num_u, IinqUpdateFieldList fieldList) {
        this.tableId = tableId;
        if (where != null) {
            this.numWheres = where.getNum_conditions();
            this.where = where;
        } else {
            numWheres = 0;
        }
        numUpdates = num_u;
        this.updateFieldList = fieldList;
    }

    public String generateFunctionCall() {
        StringBuilder functionCall = new StringBuilder();
        functionCall.append("update(");
        functionCall.append(tableId);
        functionCall.append(", ");
        functionCall.append(numWheres);
        functionCall.append(", ");
        functionCall.append(numUpdates);
        if (numWheres > 0) {
            functionCall.append(", ");
            functionCall.append(where.generateIinqConditionList());
        }
        functionCall.append(", ");
        functionCall.append(updateFieldList.generateUpdateList());
        functionCall.append(")");

        return functionCall.toString();
    }
}

