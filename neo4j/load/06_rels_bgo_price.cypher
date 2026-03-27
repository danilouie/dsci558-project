CALL {
  LOAD CSV WITH HEADERS FROM 'file:///bgo_price_edges.csv' AS row
  MATCH (k:BGOKey {key: row.bgo_key})
  MATCH (p:PricePoint {price_point_id: row.price_point_id})
  MERGE (k)-[:HAS_PRICE_POINT]->(p)
} IN TRANSACTIONS OF 200 ROWS;
