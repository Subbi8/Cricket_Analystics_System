CREATE DATABASE cricket_db;

CREATE TABLE batting_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    player_name VARCHAR(100),
    total_runs INTEGER,
    balls_faced INTEGER,
    strike_rate DECIMAL(10,2),
    PRIMARY KEY (window_start, window_end, player_name)
);

CREATE TABLE bowling_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    bowler_name VARCHAR(100),
    wickets INTEGER,
    runs_conceded INTEGER,
    economy_rate DECIMAL(10,2),
    PRIMARY KEY (window_start, window_end, bowler_name)
);