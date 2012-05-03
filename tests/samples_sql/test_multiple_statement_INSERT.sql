INSERT INTO test_multiple_statement_INSERT (key) VALUES (:key);
UPDATE test_multiple_statement_INSERT SET value = :key WHERE key = :key;
