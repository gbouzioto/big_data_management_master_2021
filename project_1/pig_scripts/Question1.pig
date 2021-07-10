-- Load data
data = LOAD 'athlete_clean' USING PigStorage('\t') AS (id:long, name:chararray, sex:chararray, age:int, height:int, weight:int, team:chararray, noc:chararray, games:chararray, year:int, season:chararray, city:chararray, sport:chararray, event:chararray, medal:chararray);

-- Keep only gold medals
filtered_data = FILTER data BY medal == 'Gold';

-- Group results
grouped_data = GROUP filtered_data BY (id, name, sex);

-- Generate gold medal counts
result = FOREACH grouped_data GENERATE group.id, group.name, group.sex, COUNT(filtered_data.medal) AS gold_count;

-- Order results
ordered_result = ORDER result BY id ASC;

-- Store results
STORE ordered_result INTO 'question1' USING PigStorage('\t');
