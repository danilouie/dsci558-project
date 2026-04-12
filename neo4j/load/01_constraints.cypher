CREATE CONSTRAINT game_bgg_id IF NOT EXISTS FOR (g:Game) REQUIRE g.bgg_id IS UNIQUE;
CREATE CONSTRAINT pricepoint_id IF NOT EXISTS FOR (p:PricePoint) REQUIRE p.price_point_id IS UNIQUE;
CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE;
CREATE CONSTRAINT user_username IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE;
CREATE CONSTRAINT bggreview_id IF NOT EXISTS FOR (b:BggReview) REQUIRE b.bgg_review_id IS UNIQUE;

CREATE INDEX game_name IF NOT EXISTS FOR (g:Game) ON (g.name);
CREATE INDEX pricepoint_date IF NOT EXISTS FOR (p:PricePoint) ON (p.date);
CREATE INDEX review_published IF NOT EXISTS FOR (r:Review) ON (r.published_at);
CREATE INDEX bggreview_username IF NOT EXISTS FOR (b:BggReview) ON (b.username);
