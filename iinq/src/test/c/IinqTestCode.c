/******************************************************************************/
/**
		@file		iinq_user.c
		@author		Dana Klamut, Kai Neubauer

@brief		This code contains definitions for iinq user functions
@copyright	Copyright 2017
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

#include "iinq_user.h"

void
cleanup(
) {
	fremove("1.ffs");
	fremove("2.ffs");
	fremove("3.ffs");
	fremove("6.ffs");
	fremove("SEL.inq");
	fremove("DEL.inq");
	fremove("UPD.inq");
	fremove("ion_mt.tbl");
}

// TODO: test OR and XOR once implemented
int
main(
		void
) {
	/* Clean-up */
	cleanup();

	/* Test CREATE TABLE statement */
	SQL_execute("CREATE TABLE Dogs (id INT, type CHAR(20), name VARCHAR(30), age INT, city VARCHAR(30), primary key(id));");

	/* Test INSERT statements */
	SQL_execute("INSERT INTO Dogs VALUES (10, 'Frenchie', 'Minnie', 1, 'Penticton');");
	SQL_execute("INSERT INTO Dogs VALUES (40, 'Chihuahua', 'Barky', 7, 'Van');");
	SQL_execute("INSERT INTO Dogs COLUMNS (id, type, age) VALUES (30, 'Black Lab', 5);");
	SQL_execute("INSERT INTO Dogs COLUMNS (id, type) VALUES (30, 'Black Lab');");
	SQL_execute("INSERT INTO Dogs COLUMNS (city, name, id) VALUES ('West Bench', 'Corky', 50);");

	/* Test UPDATE statement */
	SQL_execute("UPDATE Dogs SET id = id-1, age = age * 10 WHERE name = 'Barky';");

	/* Test DELETE statement */
	SQL_execute("DELETE FROM Dogs WHERE age < 5 AND age > 2;");

	/* Test DROP TABLE statement */
	SQL_execute("DROP TABLE Dogs;");

	/* Create Dogs table for further testing */
	SQL_execute("CREATE TABLE Dogs (id INT, type CHAR(20), name VARCHAR(30), age INT, city VARCHAR(30), primary key(id));");

	/* Test prepared statements */
	iinq_prepared_sql p1 = SQL_prepare("INSERT INTO Dogs VALUES (10, (?), 'Minnie', (?), 'Penticton');");

	/* Test that multiple tables simultaneously will not break functionality */
	SQL_execute("CREATE TABLE Cats (id VARCHAR(2), name VARCHAR(30), age INT, primary key(id));");

	iinq_prepared_sql p2 = SQL_prepare("INSERT INTO Cats VALUES ('1', ?, (?));");

	/* Test DELETE with multiple conditions */
	SQL_execute("DELETE FROM Cats WHERE id >= 5 AND id < 10 AND name != 'Minnie';");

	/* Test UPDATE with multiple conditions */
	SQL_execute("UPDATE Cats SET age = age + 90 WHERE id >= 5 AND id < 10 AND name != 'Minnie';");
	SQL_execute("UPDATE Cats SET age = 90 WHERE age < 5;");

	/* Test update with implicit fields */
	SQL_execute("UPDATE Cats SET age = 90, id = id+1, name = 'Chichi' WHERE age < 5;");
	SQL_execute("UPDATE Cats SET age = 90, id = id+1, name = 'Chichi' WHERE id >= 5 AND id < 10 AND name != 'Minnie';");

	iinq_prepared_sql p4 = SQL_prepare("INSERT INTO Cats VALUES ('3', 'Buttons', 2);");

	iinq_prepared_sql p5 = SQL_prepare("INSERT INTO Cats VALUES ('4', 'Mr. Whiskers', 4);");

	SQL_execute("UPDATE Cats SET age = age + 5 WHERE age > 2;");

	SQL_execute("DELETE FROM Cats WHERE age >= 10;");
	/* Test query */
	iinq_result_set rs1 = SQL_select("SELECT id, name FROM Cats WHERE age < 10;");

	printf("sizeof value: %zu\n", (sizeof(int) * 2) + (sizeof(char) * 30));

	SQL_execute("DROP TABLE Cats;");

	/* Clean-up */
	cleanup();

	return 0;
}
