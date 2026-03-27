CALL {
  LOAD CSV WITH HEADERS FROM 'file:///games.csv' AS row
  MERGE (g:Game {bgg_id: row.bgg_id})
  SET g += row
  SET
    g.name = row.name,
    g.year = CASE WHEN row.year = '' THEN null ELSE toInteger(row.year) END,
    g.rank = CASE WHEN row.rank = '' THEN null ELSE toInteger(row.rank) END,
    g.geek_rating = CASE WHEN row.geek_rating = '' THEN null ELSE toFloat(row.geek_rating) END,
    g.avg_rating = CASE WHEN row.avg_rating = '' THEN null ELSE toFloat(row.avg_rating) END,
    g.num_voters = CASE WHEN row.num_voters = '' THEN null ELSE toInteger(row.num_voters) END,
    g.is_expansion = (row.is_expansion = 'True' OR row.is_expansion = 'true' OR row.is_expansion = '1'),
    g.description = row.description,
    g.min_players = CASE WHEN row.min_players = '' THEN null ELSE toInteger(row.min_players) END,
    g.max_players = CASE WHEN row.max_players = '' THEN null ELSE toInteger(row.max_players) END,
    g.best_min_players = CASE WHEN row.best_min_players = '' THEN null ELSE toInteger(row.best_min_players) END,
    g.best_max_players = CASE WHEN row.best_max_players = '' THEN null ELSE toInteger(row.best_max_players) END,
    g.min_playtime = CASE WHEN row.min_playtime = '' THEN null ELSE toInteger(row.min_playtime) END,
    g.max_playtime = CASE WHEN row.max_playtime = '' THEN null ELSE toInteger(row.max_playtime) END,
    g.min_age = CASE WHEN row.min_age = '' THEN null ELSE toInteger(row.min_age) END,
    g.complexity = CASE WHEN row.complexity = '' THEN null ELSE toFloat(row.complexity) END,
    g.categories = CASE WHEN coalesce(row.categories,'') = '' THEN [] ELSE split(row.categories, '|') END,
    g.mechanisms = CASE WHEN coalesce(row.mechanisms,'') = '' THEN [] ELSE split(row.mechanisms, '|') END,
    g.abstracts_rank = CASE WHEN row.abstracts_rank = '' THEN null ELSE toInteger(row.abstracts_rank) END,
    g.cgs_rank = CASE WHEN row.cgs_rank = '' THEN null ELSE toInteger(row.cgs_rank) END,
    g.childrensgames_rank = CASE WHEN row.childrensgames_rank = '' THEN null ELSE toInteger(row.childrensgames_rank) END,
    g.familygames_rank = CASE WHEN row.familygames_rank = '' THEN null ELSE toInteger(row.familygames_rank) END,
    g.partygames_rank = CASE WHEN row.partygames_rank = '' THEN null ELSE toInteger(row.partygames_rank) END,
    g.strategygames_rank = CASE WHEN row.strategygames_rank = '' THEN null ELSE toInteger(row.strategygames_rank) END,
    g.thematic_rank = CASE WHEN row.thematic_rank = '' THEN null ELSE toInteger(row.thematic_rank) END,
    g.wargames_rank = CASE WHEN row.wargames_rank = '' THEN null ELSE toInteger(row.wargames_rank) END
} IN TRANSACTIONS OF 200 ROWS;
