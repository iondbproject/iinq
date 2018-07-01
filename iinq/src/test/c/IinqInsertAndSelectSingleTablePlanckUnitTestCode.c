/******************************************************************************/
/**
@file		test_iinq_device.c
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

#define OUTPUT_QUERY_RESULTS    0
#define OUTPUT_TIMES            0
#define OUTPUT_SQL_STATEMENTS   0

void
test_create_table1(
		planck_unit_test_t *tc
) {
    #if OUTPUT_SQL_STATEMENTS
    printf("CREATE TABLE Table1 (ID INT, CharValue VARCHAR(30), IntValue INT, primary key(ID));\n");
    #endif

    ion_err_t error =
    SQL_execute("CREATE TABLE Table1 (ID INT, CharValue VARCHAR(30), IntValue INT, primary key(ID));");

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, err_ok, error);

	ion_dictionary_t dictionary;
	ion_dictionary_handler_t handler;

	iinq_open_source(0, &dictionary, &handler);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, key_type_numeric_signed, dictionary.instance->key_type);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), dictionary.instance->record.key_size);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, (sizeof(int) * 2)+(sizeof(char) * 31), dictionary.instance->record.value_size);
	PLANCK_UNIT_ASSERT_TRUE(tc, iinq_is_key_field(0, 1));
	PLANCK_UNIT_ASSERT_FALSE(tc, iinq_is_key_field(0, 2));
	PLANCK_UNIT_ASSERT_FALSE(tc, iinq_is_key_field(0, 3));
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, iinq_int, getFieldType(0, 1));
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, iinq_null_terminated_string, getFieldType(0, 2));
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, iinq_int, getFieldType(0, 3));
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, calculateOffset(0, 1));
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), calculateOffset(0, 2));
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, calculateOffset(0, 3));
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)*2+sizeof(char)*31, calculateOffset(0, 4));
}

void
test_record_exists_table1(
    planck_unit_test_t  *tc,
	int		id
) {
    ion_value_t value = malloc(sizeof(int)*2 + sizeof(char) * 31);
	ion_dictionary_t dictionary;
	ion_dictionary_handler_t handler;
	ion_err_t error = iinq_open_source(0, &dictionary, &handler);

	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, err_ok, error);

	ion_status_t status = dictionary_get(&dictionary, IONIZE(id, int), value);
	free(value);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, err_ok, status.error);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, status.count);
}

void
test_insert_prepared_record_table1(
    planck_unit_test_t *tc,
	int		id,
	char	*char_value,
	int		int_value
) {
	iinq_prepared_sql *p = SQL_prepare("INSERT INTO Table1 VALUES (?, ?, ?);");
    setParam(p, 1, IONIZE(id, int));
    setParam(p, 2, char_value);
    setParam(p, 3, IONIZE(int_value, int));

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, p->table)
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, id, NEUTRALIZE(p->value, int))
    PLANCK_UNIT_ASSERT_STR_ARE_EQUAL(tc, char_value, (char*) (p->value+sizeof(int)))
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, int_value, NEUTRALIZE(p->value+sizeof(int)+sizeof(char)*31, int))

    ion_err_t error = execute(p);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, err_ok, error);

    iinq_close_statement(p);

	test_record_exists_table1(tc, id);
}

void
test_insert_record_table1(
    planck_unit_test_t  *tc,
	int		id,
	char	*char_value,
	int		int_value
) {
	iinq_execute_instantaneous(iinq_insert_0(id, char_value, int_value));

    PLANCK_UNIT_ASSERT_TRUE(tc, iinq_reserved == NULL);
    test_record_exists_table1(tc, id);
}

void
test_insert_records_table1(
    planck_unit_test_t      *tc
) {
    int                     i;
	volatile unsigned long	start_time, end_time;

	start_time = ion_time();

	for (i = 0; i < num_records; i++) {
		test_insert_record_table1(tc, i + 1, "regInsert", i + 5);
	}

	end_time = ion_time();
    #if OUTPUT_TIMES
	printf("%d records inserted. Time taken: %lu\n", num_records, end_time - start_time);
	#endif
}

void
test_insert_records_prep_table1(
    planck_unit_test_t      *tc
) {
	int                     i;
	volatile unsigned long	start_time, end_time;

	start_time = ion_time();

	for (i = 0; i < num_records; i++) {
		test_insert_prepared_record_table1(tc, -i, "prepInsert", -i + 5);
	}

	end_time = ion_time();
    #if OUTPUT_TIMES
	printf("%d records inserted. Time taken: %lu\n", num_records, end_time - start_time);
	#endif
}

void
test_select_all_records_table1(
    planck_unit_test_t      *tc
) {
	volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1\n");
    #endif

	start_time = ion_time();

	iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1;");

	end_time = ion_time();
	#if OUTPUT_TIMES
	printf("Time taken: %lu\n\n", end_time - start_time);
	#endif

	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->num_wheres);
	PLANCK_UNIT_ASSERT_TRUE(tc, NULL == rs1->wheres);

	PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    int count1 = 0;
    int count2 = 0;
	while (iinq_next(rs1)) {
	    #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
        #endif
        if (count1 < num_records) {
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, count1 + 1, iinq_get_int(rs1, 1));
            PLANCK_UNIT_ASSERT_STR_ARE_EQUAL(tc, "regInsert", iinq_get_string(rs1, 2));
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, count1 + 5, iinq_get_int(rs1, 3));
            count1++;
        } else {
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, -count2, iinq_get_int(rs1, 1));
            PLANCK_UNIT_ASSERT_STR_ARE_EQUAL(tc, "prepInsert", iinq_get_string(rs1, 2));
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, -count2 + 5, iinq_get_int(rs1, 3));
            count2++;
        }
	}
    #if OUTPUT_QUERY_RESULTS
	printf("\n");
	#endif

	iinq_close_result_set(rs1);
}

void
test_select_field_list_table1(
    planck_unit_test_t      *tc
) {
    volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT IntValue, ID FROM Table1\n");
    #endif

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT IntValue, ID FROM Table1;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[1]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_TRUE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[1]);

    int count1 = 0;
    int count2 = 0;

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("IntValue: %d\n", iinq_get_int(rs1, 1));
        printf("ID: %i, ", iinq_get_int(rs1, 2));
        #endif
        if (count1 < num_records) {
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, count1 + 5, iinq_get_int(rs1, 1));
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, count1 + 1, iinq_get_int(rs1, 2));
            count1++;
        } else {
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, -count2 + 5, iinq_get_int(rs1, 1));
            PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, -count2, iinq_get_int(rs1, 2));
            count2++;
        }
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif

    iinq_close_result_set(rs1);
}

void
test_select_all_where_greater_than_table1(
    planck_unit_test_t      *tc
) {
    volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1 WHERE ID > 50;\n");
    #endif

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID > 50;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_FALSE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
        #endif
        PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 1) > 50);
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif
}

void
test_select_all_where_greater_than_equal_table1(
    planck_unit_test_t      *tc
) {
    volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1 WHERE ID >= 50;\n");
    #endif

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID >= 50;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_FALSE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
        #endif
        PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 1) >= 50);
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif

    iinq_close_result_set(rs1);
}

void
test_select_all_where_less_than_table1(
    planck_unit_test_t      *tc
) {
    volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1 WHERE ID < 50;\n");
    #endif

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID < 50;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_FALSE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
        #endif
        PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 1) < 50);
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif

    iinq_close_result_set(rs1);
}

void
test_select_all_where_less_than_equal_table1(
    planck_unit_test_t      *tc
) {
    volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1 WHERE ID <= 50;\n");
    #endif

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID <= 50;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_FALSE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
        #endif
        PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 1) <= 50);
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif

    iinq_close_result_set(rs1);
}

void
test_select_all_where_not_equal_table1(
    planck_unit_test_t      *tc
) {
    volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1 WHERE ID <> 50;\n");
    #endif

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID <> 50;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_FALSE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
        #endif
        PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 1) != 50);
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif

    iinq_close_result_set(rs1);

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1 WHERE ID != 50;\n");
    #endif

    start_time = ion_time();
    rs1 = SQL_select("SELECT * FROM Table1 WHERE ID <> 50;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_FALSE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
        #endif
        PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 1) != 50);
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif

    iinq_close_result_set(rs1);
}

void
test_select_all_where_multiple_conditions_table1(
    planck_unit_test_t      *tc
) {
    volatile unsigned long start_time, end_time;

    #if OUTPUT_SQL_STATEMENTS
    printf("SELECT * FROM Table1 WHERE ID < 50 AND IntValue <> 50;\n");
    #endif

    start_time = ion_time();

    iinq_result_set *rs1 = SQL_select("SELECT * FROM Table1 WHERE ID < 50 AND IntValue <> 50;");

    end_time = ion_time();
    #if OUTPUT_TIMES
    printf("Time taken: %lu\n\n", end_time - start_time);
    #endif

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->num_fields);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 1, rs1->fields[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->fields[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 3, rs1->fields[2]);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 2, rs1->num_wheres);
    PLANCK_UNIT_ASSERT_FALSE(tc, NULL == rs1->wheres);

    PLANCK_UNIT_ASSERT_FALSE(tc, rs1->dictionary_ref.temp_dictionary);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->table_id);

    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, 0, rs1->offset[0]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int), rs1->offset[1]);
    PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, sizeof(int)+sizeof(char)*31, rs1->offset[2]);

    while (iinq_next(rs1)) {
        #if OUTPUT_QUERY_RESULTS
        printf("ID: %i, ", iinq_get_int(rs1, 1));
        printf("CharValue: %s, ", iinq_get_string(rs1, 2));
        printf("IntValue: %d\n", iinq_get_int(rs1, 3));
		#endif
		PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 1) < 50);
		PLANCK_UNIT_ASSERT_TRUE(tc, iinq_get_int(rs1, 3) != 50);
    }
    #if OUTPUT_QUERY_RESULTS
    printf("\n");
    #endif

    iinq_close_result_set(rs1);
}

void
test_drop_table1(
    planck_unit_test_t      *tc
) {
    #if OUTPUT_SQL_STATEMENTS
	printf("DROP TABLE Table1;");
	#endif

    ion_err_t error =
	SQL_execute("DROP TABLE Table1;");

	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, err_ok, error);

	ion_dictionary_t dictionary;
	ion_dictionary_handler_t handler;

	error = iinq_open_source(0, &dictionary, &handler);

	PLANCK_UNIT_ASSERT_INT_ARE_EQUAL(tc, err_file_open_error, error);
}

planck_unit_suite_t *
iinq_get_suite1(
) {
	planck_unit_suite_t *suite = planck_unit_new_suite();

	PLANCK_UNIT_ADD_TO_SUITE(suite, test_create_table1);
	PLANCK_UNIT_ADD_TO_SUITE(suite, test_insert_records_table1);
	PLANCK_UNIT_ADD_TO_SUITE(suite, test_insert_records_prep_table1);
	PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_all_records_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_field_list_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_all_where_greater_than_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_all_where_greater_than_equal_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_all_where_less_than_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_all_where_less_than_equal_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_all_where_not_equal_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_select_all_where_multiple_conditions_table1);
    PLANCK_UNIT_ADD_TO_SUITE(suite, test_drop_table1);

	return suite;
}

void
run_all_tests_iinq_device(
	int records
) {
	num_records = records;

	planck_unit_suite_t *suite1 = iinq_get_suite1();

	planck_unit_run_suite(suite1);
	planck_unit_destroy_suite(suite1);

	fdeleteall();
	fremove(ION_MASTER_TABLE_FILENAME);
}
