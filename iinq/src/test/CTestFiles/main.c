#include <stdio.h>
#include "SQL.h"

int
main() {
    printf("Hello, World!\n");
    printf("Testing SQL\n");
    executeSQL("SELECT * FROM IINQTestSO;");
                         //executeSQL("SELECT PartDB.Part.P_NAME, OrderDB.LineItem.L_QUANTITY, OrderDB.Customer.C_Name, PartDB.Supplier.s_name FROM OrderDB.CUSTOMER, OrderDB.LINEITEM, OrderDB.ORDERS, PartDB.PART, PartDB.Supplier WHERE OrderDB.LINEITEM.L_PARTKEY = PartDB.PART.P_PARTKEY AND OrderDB.LINEITEM.L_ORDERKEY = OrderDB.ORDERS.O_ORDERKEY AND OrderDB.ORDERS.O_CUSTKEY = OrderDB.CUSTOMER.C_CUSTKEY and PartDB.supplier.s_suppkey = OrderDB.lineitem.l_suppkey AND OrderDB.Customer.C_Name = 'Customer#000000025';");
    /*executeSQL("SELECT PartDB.Part.P_NAME, OrderDB.LineItem.L_QUANTITY, OrderDB.Customer.C_Name, PartDB.Supplier.s_name FROM OrderDB.CUSTOMER, OrderDB.LINEITEM, OrderDB.ORDERS, PartDB.PART, PartDB.Supplier WHERE OrderDB.LINEITEM.L_PARTKEY = PartDB.PART.P_PARTKEY AND OrderDB.LINEITEM.L_ORDERKEY = OrderDB.ORDERS.O_ORDERKEY AND OrderDB.ORDERS.O_CUSTKEY = OrderDB.CUSTOMER.C_CUSTKEY and PartDB.supplier.s_suppkey = OrderDB.lineitem.l_suppkey AND OrderDB.Customer.C_Name = 'Customer#000000025';");*/

    return 0;
}