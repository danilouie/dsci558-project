// Quick sanity checks
MATCH (g:Game) RETURN count(g) AS games;
MATCH (p:PricePoint) RETURN count(p) AS price_points;
MATCH (r:Review) RETURN count(r) AS reviews;

MATCH (p:PricePoint)
WHERE NOT EXISTS { MATCH (:Game)-[:HAS_PRICE_POINT]->(p) }
RETURN count(p) AS orphan_price_points_from_game;

MATCH (r:Review)
WHERE NOT EXISTS { MATCH (:Game)-[:HAS_REVIEW]->(r) }
RETURN count(r) AS orphan_reviews;
