-- Grant the test user full privileges across databases so cross-schema
-- (cross-database, in MySQL terms) tests can CREATE / DROP additional
-- databases and reference tables that live in them.
GRANT ALL PRIVILEGES ON *.* TO 'username'@'%';
FLUSH PRIVILEGES;
