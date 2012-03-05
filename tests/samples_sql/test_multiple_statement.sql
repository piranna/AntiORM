UPDATE test_multiple_statement SET key = 'b' WHERE key = 'a';
UPDATE test_multiple_statement SET key = :key WHERE key = 'b';
