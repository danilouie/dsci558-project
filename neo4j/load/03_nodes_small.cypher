CALL {
  LOAD CSV WITH HEADERS FROM 'file:///ranks.csv' AS row
  MATCH (g:Game {bgg_id: row.bgg_id})
  SET g += row
  SET
    g.rank = CASE WHEN row.rank = '' THEN g.rank ELSE toInteger(row.rank) END,
    g.rank_value = CASE WHEN row.rank_value = '' THEN null ELSE toInteger(row.rank_value) END,
    g.bayesaverage = CASE WHEN row.bayesaverage = '' THEN null ELSE toFloat(row.bayesaverage) END,
    g.average = CASE WHEN row.average = '' THEN null ELSE toFloat(row.average) END,
    g.usersrated = CASE WHEN row.usersrated = '' THEN null ELSE toInteger(row.usersrated) END,
    g.is_expansion = CASE WHEN row.is_expansion = '' THEN g.is_expansion ELSE (row.is_expansion = 'True' OR row.is_expansion = 'true' OR row.is_expansion = '1') END,
    g.yearpublished = CASE WHEN row.yearpublished = '' THEN null ELSE toInteger(row.yearpublished) END,
    g.abstracts_rank = CASE WHEN row.abstracts_rank = '' THEN null ELSE toInteger(row.abstracts_rank) END,
    g.cgs_rank = CASE WHEN row.cgs_rank = '' THEN null ELSE toInteger(row.cgs_rank) END,
    g.childrensgames_rank = CASE WHEN row.childrensgames_rank = '' THEN null ELSE toInteger(row.childrensgames_rank) END,
    g.familygames_rank = CASE WHEN row.familygames_rank = '' THEN null ELSE toInteger(row.familygames_rank) END,
    g.partygames_rank = CASE WHEN row.partygames_rank = '' THEN null ELSE toInteger(row.partygames_rank) END,
    g.strategygames_rank = CASE WHEN row.strategygames_rank = '' THEN null ELSE toInteger(row.strategygames_rank) END,
    g.thematic_rank = CASE WHEN row.thematic_rank = '' THEN null ELSE toInteger(row.thematic_rank) END,
    g.wargames_rank = CASE WHEN row.wargames_rank = '' THEN null ELSE toInteger(row.wargames_rank) END
} IN TRANSACTIONS OF 1000 ROWS;

CALL {
  LOAD CSV WITH HEADERS FROM 'file:///reviews.csv' AS row
  MERGE (r:Review {review_id: row.review_id})
  SET r += row
  SET
    r.url = row.url,
    r.title = row.title,
    r.author = row.author,
    r.category = row.category,
    r.published_at = CASE WHEN row.published_at = '' THEN null ELSE datetime(row.published_at) END,
    r.score = CASE WHEN row.score = '' THEN null ELSE toFloat(row.score) END,
    r.game_name_raw = row.game_name_raw
} IN TRANSACTIONS OF 500 ROWS;
