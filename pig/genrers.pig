data = LOAD '/home/aquila/Documents/engdados/hadoop-studies-master/pig/df_netflix.csv' 
USING PigStorage('|') 
AS 
(index:chararray,
id:chararray, 
title:chararray, 
type:chararray, 
desciption:chararray, 
release_year:int, 
age_certification:chararray,
runtime:int,
genres:chararray,
production_country:chararray,
seasons:float,
imdb_id:chararray,
imdb_score:float,
imdb_votes:chararray,
tmdb_popularity:chararray,
tmdb_score:chararray
);

filter_animation = FILTER data BY (genres matches '.*animation.*');
filter_country = FILTER filter_animation BY (production_country matches '.*JP.*');
filter_score = FILTER filter_country BY imdb_score >= 7.0;

filter_name = ORDER filter_score BY imdb_score;
get_result = FOREACH filter_name GENERATE title, release_year;

DUMP get_result;

STORE get_result INTO 'out/' USING PigStorage('|');