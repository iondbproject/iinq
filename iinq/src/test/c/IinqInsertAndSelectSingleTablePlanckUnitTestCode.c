/******************************************************************************/
/**
@file		test_iinq.c
@author		IonDB Project
@brief		iinq tests.
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

#include "test_iinq_device.h"

int num_records = 100;

void
create_table1(
) {
	SQL_execute("CREATE TABLE Table1 (ID INT, CharValue VARCHAR(30), IntValue INT, primary key(ID));");
	printf("Table created.\n");
}

void
insert_prepared_record(
	int		id,
	char	*char_value,
	int		int_value
) {
	iinq_prepared_sql *p = SQL_prepare("INSERT INTO Table1 VALUES (?, ?, ?);");
    setParam(p, 1, IONIZE(id, int));
    setParam(p, 2, char_value);
    setParam(p, 3, IONIZE(int_value, int));

	execute(p);
}

void
insert_record(
	int		id,
	char	*char_value,
	int		int_value
) {
	iinq_execute_instantaneous(iinq_insert_0(id, char_value, int_value));
}

void
insert_records(
) {
    int                     i;
	volatile unsigned long	start_time, end_time;

	printf("Inserting records.\n");
	start_time = ion_time();

	for (i = 0; i < num_records; i++) {
		insert_record(i + 1, "regInsert", i + 5);
	}

	end_time = ion_time();

	printf("%d records inserted. Time taken: %lu\n", num_records, end_time - start_time);
}

void
insert_records_prep(
) {
	int                     i;
	volatile unsigned long	start_time, end_time;

	printf("Inserting prep records.\n");
	start_time = ion_time();

	for (i = 0; i < num_records; i++) {
		insert_prepared_record(-i, "prepInsert", -i + 5);
	}

	end_time = ion_time();

	printf("%d records inserted. Time taken: %lu\n", num_records, end_time - start_time);
}

void
select_all_records(
) {
	volatile unsigned long start_time, end_time;

    printf("SELECT * FROM Table1\n");

	start_time = ion_time();

	iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1;");

	end_time = ion_time();
	printf("Time taken: %lu\n\n", end_time - start_time);

	while (iinq_next(rs1)) {
		printf("ID: %i, ", iinq_get_int(rs1, 1));
		printf("CharValue: %s, ", iinq_get_string(rs1, 2));
		printf("IntValue: %d\n", iinq_get_int(rs1, 3));
	}

	printf("\n");
}

void
select_field_list(
) {
    volatile unsigned long start_time, end_time;

    printf("SELECT IntValue, ID FROM Table1\n");

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT IntValue, ID FROM Table1;");

    end_time = ion_time();
    printf("Done select\n");
    printf("Time taken: %lu\n\n", end_time - start_time);

    while (iinq_next(rs1)) {
        printf("IntValue: %d\n", iinq_get_int(rs1, 1));
		printf("ID: %i, ", iinq_get_int(rs1, 2));
    }

    printf("\n");
}

void
select_all_where_greater_than(
) {
    volatile unsigned long start_time, end_time;

    printf("SELECT * FROM Table1 WHERE ID > 50;\n");

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID > 50;");

    end_time = ion_time();
    printf("Time taken: %lu\n\n", end_time - start_time);

    while (iinq_next(rs1)) {
		printf("ID: %i, ", iinq_get_int(rs1, 1));
		printf("CharValue: %s, ", iinq_get_string(rs1, 2));
		printf("IntValue: %d\n", iinq_get_int(rs1, 3));
    }

    printf("\n");
}

void
select_all_where_greater_than_equal(
) {
    volatile unsigned long start_time, end_time;

    printf("SELECT * FROM Table1 WHERE ID >= 50;\n");

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID >= 50;");

    end_time = ion_time();
    printf("Time taken: %lu\n\n", end_time - start_time);

    while (iinq_next(rs1)) {
		printf("ID: %i, ", iinq_get_int(rs1, 1));
		printf("CharValue: %s, ", iinq_get_string(rs1, 2));
		printf("IntValue: %d\n", iinq_get_int(rs1, 3));
    }

    printf("\n");
}

void
select_all_where_less_than(
) {
    volatile unsigned long start_time, end_time;

    printf("SELECT * FROM Table1 WHERE ID < 50;\n");

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID < 50;");

    end_time = ion_time();
    printf("Time taken: %lu\n\n", end_time - start_time);

    while (iinq_next(rs1)) {
		printf("ID: %i, ", iinq_get_int(rs1, 1));
		printf("CharValue: %s, ", iinq_get_string(rs1, 2));
		printf("IntValue: %d\n", iinq_get_int(rs1, 3));
    }

    printf("\n");
}

void
select_all_where_less_than_equal(
) {
    volatile unsigned long start_time, end_time;

    printf("SELECT * FROM Table1 WHERE ID <= 50;\n");

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID <= 50;");

    end_time = ion_time();
    printf("Time taken: %lu\n\n", end_time - start_time);

    while (iinq_next(rs1)) {
		printf("ID: %i, ", iinq_get_int(rs1, 1));
		printf("CharValue: %s, ", iinq_get_string(rs1, 2));
		printf("IntValue: %d\n", iinq_get_int(rs1, 3));
    }

    printf("\n");
}

void
select_all_where_not_equal(
) {
    volatile unsigned long start_time, end_time;

    printf("SELECT * FROM Table1 WHERE ID <> 50;\n");

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID <> 50;");

    end_time = ion_time();
    printf("Time taken: %lu\n\n", end_time - start_time);

    while (iinq_next(rs1)) {
		printf("ID: %i, ", iinq_get_int(rs1, 1));
		printf("CharValue: %s, ", iinq_get_string(rs1, 2));
		printf("IntValue: %d\n", iinq_get_int(rs1, 3));
    }

    printf("\n");
}

void
select_all_where_multiple_conditions(
) {
    volatile unsigned long start_time, end_time;

    printf("SELECT * FROM Table1 WHERE ID < 50 AND IntValue <> 50;\n");

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID < 50 AND IntValue <> 50;");

    end_time = ion_time();
    printf("Time taken: %lu\n\n", end_time - start_time);

    while (iinq_next(rs1)) {
		printf("ID: %i, ", iinq_get_int(rs1, 1));
		printf("CharValue: %s, ", iinq_get_string(rs1, 2));
		printf("IntValue: %d\n", iinq_get_int(rs1, 3));
    }

    printf("\n");
}

void
drop_table1(
) {
	SQL_execute("DROP TABLE Table1;");
}

planck_unit_suite_t *
iinq_get_suite1(
) {
	int i;

	planck_unit_suite_t *suite = planck_unit_new_suite();

	PLANCK_UNIT_ADD_TO_SUITE(suite, create_table1);
	PLANCK_UNIT_ADD_TO_SUITE(suite, insert_records);
	PLANCK_UNIT_ADD_TO_SUITE(suite, insert_records_prep);
	PLANCK_UNIT_ADD_TO_SUITE(suite, select_all_records);
    PLANCK_UNIT_ADD_TO_SUITE(suite, select_field_list);
    PLANCK_UNIT_ADD_TO_SUITE(suite, select_all_where_greater_than);
    PLANCK_UNIT_ADD_TO_SUITE(suite, select_all_where_greater_than_equal);
    PLANCK_UNIT_ADD_TO_SUITE(suite, select_all_where_less_than);
    PLANCK_UNIT_ADD_TO_SUITE(suite, select_all_where_less_than_equal);
    PLANCK_UNIT_ADD_TO_SUITE(suite, select_all_where_not_equal);
    PLANCK_UNIT_ADD_TO_SUITE(suite, select_all_where_multiple_conditions);
    PLANCK_UNIT_ADD_TO_SUITE(suite, drop_table1);

	return suite;
}

void
run_all_tests_iinq_device(
	int records
) {
	printf("num records in table: %i\n", records);
	num_records = records;

	planck_unit_suite_t *suite1 = iinq_get_suite1();

	planck_unit_run_suite(suite1);
	planck_unit_destroy_suite(suite1);

	fdeleteall();
	fremove(ION_MASTER_TABLE_FILENAME);
}
