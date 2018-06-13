/******************************************************************************/
/**
 @file		    IinqInsert.java
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

import iinq.functions.PreparedInsertFunction;
import iinq.metadata.IinqTable;
import unity.annotation.SourceField;
import unity.query.GQFieldRef;
import unity.query.LQExprNode;
import unity.query.LQInsertNode;

import java.util.ArrayList;
import java.util.HashMap;

public class IinqInsert {
    private PreparedInsertFunction insertFunction;
    private int tableId;
    private String functionArguments;
    private boolean duplicate;
    private boolean preparedStatement;

    public IinqInsert(
            IinqTable table,
            LQInsertNode insertNode,
            PreparedInsertFunction insertFunction,
            boolean preparedStatement,
            boolean isDuplicate
    ) {
        this.tableId = table.getTableId();
        this.insertFunction = insertFunction;
        functionArguments = createFunctionArguments(table, insertNode, preparedStatement);
        this.preparedStatement = preparedStatement;
        duplicate = isDuplicate;
    }

    public boolean isDuplicate() {
        return duplicate;
    }

    public int getTableId() {
        return tableId;
    }

    public PreparedInsertFunction getInsertFunction() {
        return insertFunction;
    }

    public String getFunctionArguments() {
        return functionArguments;
    }

    public String getFunctionCall() {
        return insertFunction.getName() + "(" + getFunctionArguments() + ")";
    }

    private String createFunctionArguments(IinqTable table, LQInsertNode insertNode, boolean preparedStatement) {
        HashMap<Integer, String> map = new HashMap<>();
        ArrayList<GQFieldRef> insertFields = insertNode.getInsertFields();
        ArrayList<Object> insertValues = insertNode.getInsertValues();
        for (int i = 0, n = insertFields.size(); i < n; i ++) {
            InsertFieldElement arg = getArgument(insertFields.get(i), insertValues.get(i));
            if (preparedStatement && arg.value.equals("?"))
                arg.value = table.getNullValue(arg.fieldNum);
            map.put(arg.fieldNum, arg.value);
        }
        StringBuilder arguments = new StringBuilder();
        for (int i = 1, n = table.getNumFields(); i <= n; i++) {
            if (map.containsKey(i)) {
                arguments.append(map.get(i));
            } else {
                arguments.append(table.getNullValue(i));
            }
            arguments.append(", ");
        }
        arguments.setLength(arguments.length()-2);
        return arguments.toString();
    }

    private InsertFieldElement getArgument(GQFieldRef insertField, Object insertValue) {
        SourceField field = insertField.getTable().getTable().getField(insertField.getName());
        int pos = field.getOrdinalPosition();
        // TODO: can this be something other than an expression node?
        Object content = ((LQExprNode) insertValue).getContent();
        // strings need to be surrounded with quotes
        if (content instanceof String) {
            return new InsertFieldElement(pos, ((String) content).replace("\'", "\""));
        } else {
            return new InsertFieldElement(pos, content.toString());
        }
    }

    public boolean isPreparedStatement() {
        return preparedStatement;
    }
}
