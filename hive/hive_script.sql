#create table
CREATE TABLE netflix_shows (idx int,id string,title string,type string,description string,release_year int,age_certification string,runtime string,genres string,production_countries string,seasons int,imdb_id string,imdb_score double,imdb_votes int,tmdb_popularity double,tmdb_score double) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION 'tests-hive/df_netflix.csv';
#select 
SELECT title, release_year, imdb_score, imdb_votes, genres FROM netflix_shows WHERE genres LIKE '%animation%' AND imdb_votes > 6000 AND release_year >2016 AND imdb_score > 7.0;

LOAD DATA INPATH 'tests-hive/df_netflix.csv' INTO TABLE netflix_shows;
