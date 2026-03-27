CALL {
  LOAD CSV WITH HEADERS FROM 'file:///game_price_edges.csv' AS row
  MATCH (g:Game {bgg_id: row.bgg_id})
  MATCH (p:PricePoint {price_point_id: row.price_point_id})
  MERGE (g)-[:HAS_PRICE_POINT]->(p)
} IN TRANSACTIONS OF 200 ROWS;
