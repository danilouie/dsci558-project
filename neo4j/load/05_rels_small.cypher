CALL {
  LOAD CSV WITH HEADERS FROM 'file:///game_review_edges.csv' AS row
  MATCH (g:Game {bgg_id: row.bgg_id})
  MATCH (r:Review {review_id: row.review_id})
  MERGE (g)-[:HAS_REVIEW]->(r)
} IN TRANSACTIONS OF 1000 ROWS;
