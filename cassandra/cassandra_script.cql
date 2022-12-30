CREATE KEYSPACE IF NOT EXISTS ks_imdb WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
}

CREATE TABLE IF NOT EXISTS imdb(
    idx text PRIMARY KEY,
    title text,
    overview text,
    original_language text,
    vote_count int,
    vote_average double
)
COPY imdb(idx, title, overview, original_language,vote_count,vote_average) FROM 'tmdb.csv' WITH HEADER = true AND DELIMITER='|';
