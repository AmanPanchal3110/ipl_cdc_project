CREATE TABLE IF NOT EXISTS matches (
    id               INTEGER PRIMARY KEY,
    season           VARCHAR(10),
    city             VARCHAR(100),
    match_date       DATE,
    match_type       VARCHAR(50),
    player_of_match  VARCHAR(100),
    venue            VARCHAR(200),
    team1            VARCHAR(100),
    team2            VARCHAR(100),
    toss_winner      VARCHAR(100),
    toss_decision    VARCHAR(20),
    winner           VARCHAR(100),
    result           VARCHAR(20),
    result_margin    INTEGER,
    target_runs      INTEGER,
    target_overs     NUMERIC(4,1),
    super_over       CHAR(1),
    method           VARCHAR(20),
    umpire1          VARCHAR(100),
    umpire2          VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id      SERIAL PRIMARY KEY,
    match_id         INTEGER NOT NULL REFERENCES matches(id),
    inning           SMALLINT NOT NULL,
    batting_team     VARCHAR(100),
    bowling_team     VARCHAR(100),
    over             SMALLINT NOT NULL,
    ball             SMALLINT NOT NULL,
    batter           VARCHAR(100),
    bowler           VARCHAR(100),
    non_striker      VARCHAR(100),
    batsman_runs     SMALLINT,
    extra_runs       SMALLINT,
    total_runs       SMALLINT,
    extras_type      VARCHAR(20),
    is_wicket        SMALLINT,
    player_dismissed VARCHAR(100),
    dismissal_kind   VARCHAR(30),
    fielder          VARCHAR(100),
    CONSTRAINT uq_delivery UNIQUE (match_id, inning, over, ball)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_match_id ON deliveries(match_id);