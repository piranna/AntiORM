UPDATE test_multiple_statement SET name = 'b' WHERE name = 'a';
UPDATE test_multiple_statement SET name = :name WHERE name = 'b';
