CALL {
  LOAD CSV WITH HEADERS FROM 'file:///users.csv' AS row
  WITH row WHERE row.username IS NOT NULL AND trim(row.username) <> ''
  MERGE (u:User {username: trim(row.username)})
} IN TRANSACTIONS OF 2000 ROWS;
