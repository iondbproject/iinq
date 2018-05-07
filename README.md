![iinq](https://rawcdn.githack.com/iondbproject/iinq/select-all-from-dana/iinq/documentation/iinq_logo3.svg)
IonDB IINQ SQL Query Processor

You might also be interested in our sister projects, [IonDB - a key-value datastore for resource constrained systems](https://github.com/iondbproject/iondb) and [LittleD - A relational database using 1kB of RAM or less](https://github.com/graemedouglas/LittleD).

# "What is this?"

IINQ is an SQL Query Processor which utilizes the capabilities and performance of IonDB.


# "How to use with IonDB?"

Specify the input and output files/directory for Iinq as JVM options:
```
-DUSER_FILE="../../iondb/src/iinq/iinq_interface/iinq_user.c" -DFUNCTION_FILE="../../iondb/src/iinq/iinq_interface/iinq_user_functions.c" -DFUNCTION_HEADER_FILE="../../iondb/src/iinq/iinq_interface/iinq_user_functions.h" -DDIRECTORY="../../iondb/src/iinq/iinq_interface/"
```