INSERT INTO test_multiple_statement_INSERT (name) VALUES (:name);
UPDATE test_multiple_statement_INSERT SET surname = :surname WHERE name = :name;
