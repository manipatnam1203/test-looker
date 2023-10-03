CREATE OR REPLACE PROCEDURE `synthetic-eon-241312.test_stored_procedure.CREATE_TABLE_DML_TEST02`()
BEGIN
  CREATE OR REPLACE TABLE test_stored_procedure.sample_table (
    id INT64,
    name STRING,
    age INT64
  );
END;
