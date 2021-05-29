-- Define functions
REGISTER 'piggybank-0.17.0.jar';
define CSVRead org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
define CSVWrite org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Load data
data = LOAD 'athlete_events.csv' USING CSVRead AS (id:long, name:chararray, sex:chararray, age:int, height:int, weight:int, team:chararray, noc:chararray, games:chararray, year:int, season:chararray, city:chararray, sport:chararray, event:chararray, medal:chararray);

-- Count null values
grouped = GROUP data ALL;
null_values = FOREACH grouped {
null_medal = FILTER data BY medal == 'NA';
GENERATE (double) (100 - ((double) 100*COUNT(data.id)/(double) COUNT_STAR(data.id))) as missing_id, 
(double) (100 - ((double) 100*COUNT(data.name)/(double) COUNT_STAR(data.name))) as missing_name,
(double) (100 - ((double) 100*COUNT(data.sex)/(double) COUNT_STAR(data.sex))) as missing_sex,
(double) (100 - ((double) 100*COUNT(data.age)/(double) COUNT_STAR(data.age))) as missing_age,
(double) (100 - ((double) 100*COUNT(data.height)/(double) COUNT_STAR(data.height))) as missing_height,
(double) (100 - ((double) 100*COUNT(data.weight)/(double) COUNT_STAR(data.weight))) as missing_weight,
(double) (100 - ((double) 100*COUNT(data.team)/(double) COUNT_STAR(data.team))) as missing_team,
(double) (100 - ((double) 100*COUNT(data.noc)/(double) COUNT_STAR(data.noc))) as missing_noc,
(double) (100 - ((double) 100*COUNT(data.games)/(double) COUNT_STAR(data.games))) as missing_games,
(double) (100 - ((double) 100*COUNT(data.year)/(double) COUNT_STAR(data.year))) as missing_year,
(double) (100 - ((double) 100*COUNT(data.season)/(double) COUNT_STAR(data.season))) as missing_season,
(double) (100 - ((double) 100*COUNT(data.city)/(double) COUNT_STAR(data.city))) as missing_city,
(double) (100 - ((double) 100*COUNT(data.sport)/(double) COUNT_STAR(data.sport))) as missing_sport,
(double) (100 - ((double) 100*COUNT(data.event)/(double) COUNT_STAR(data.event))) as missing_event,
(double) ((double) 100*COUNT(null_medal.medal)/(double) COUNT(data.medal)) as missing_medal;
};

-- Save results
STORE null_values INTO 'preprocessing' USING CSVWrite;
