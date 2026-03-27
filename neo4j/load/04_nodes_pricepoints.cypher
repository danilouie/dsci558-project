CALL {
  LOAD CSV WITH HEADERS FROM 'file:///price_points.csv' AS row
  MERGE (p:PricePoint {price_point_id: row.price_point_id})
  SET
    p.date = date(row.date),
    p.bgg_id = CASE WHEN row.bgg_id = '' THEN null ELSE row.bgg_id END,
    p.pt_id = CASE WHEN row.pt_id = '' THEN null ELSE row.pt_id END,
    p.dt = CASE WHEN row.dt = '' THEN null ELSE row.dt END,
    p.min = CASE WHEN row.min = '' THEN null ELSE toFloat(row.min) END,
    p.mean = CASE WHEN row.mean = '' THEN null ELSE toFloat(row.mean) END,
    p.max = CASE WHEN row.max = '' THEN null ELSE toFloat(row.max) END,
    p.min_st = CASE WHEN row.min_st = '' THEN null ELSE toFloat(row.min_st) END,
    p.min_price = CASE WHEN row.min_price = '' THEN null ELSE toFloat(row.min_price) END,
    p.mean_price = CASE WHEN row.mean_price = '' THEN null ELSE toFloat(row.mean_price) END,
    p.max_price = CASE WHEN row.max_price = '' THEN null ELSE toFloat(row.max_price) END,
    p.source = row.source
} IN TRANSACTIONS OF 5 ROWS;
