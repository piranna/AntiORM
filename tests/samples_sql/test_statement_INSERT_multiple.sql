INSERT INTO test_statement_INSERT_multiple (key) VALUES (:key);
UPDATE test_statement_INSERT_multiple SET value = :key WHERE key = :key;
