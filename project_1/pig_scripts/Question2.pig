-- Load data
data = LOAD 'athlete_clean' USING PigStorage('\t') AS (id:long, name:chararray, sex:chararray, age:int, height:int, weight:int, team:chararray, noc:chararray, games:chararray, year:int, season:chararray, city:chararray, sport:chararray, event:chararray, medal:chararray);

-- Keep only relevant data
relevant_data = FOREACH data GENERATE name, sex, age, team, sport, games, medal;

-- Group results
grouped_data = GROUP relevant_data BY (name, sex, age, team, sport, games);

-- Generate medal counts
medal_count = FOREACH grouped_data {
		gold = FILTER relevant_data BY medal == 'Gold';
		silver = FILTER relevant_data BY medal == 'Silver';  
		bronze = FILTER relevant_data BY medal == 'Bronze';
		GENERATE group.name, group.sex, group.age, group.team,
		group.sport, group.games, COUNT(gold) as gold_count,
		COUNT(silver) as silver_count, COUNT(bronze) as
		bronze_count, (COUNT(gold) + COUNT(silver) + COUNT(bronze))
		as total_count;
		};

-- Rank results and get top 10 performances ordered
results = RANK medal_count BY gold_count DESC, total_count DESC DENSE;
results_10 = FILTER results BY $0 <= 10;
results_ordered = ORDER results_10 BY $0 ASC, $1 ASC;

-- Save results
STORE results_ordered INTO 'question2' USING PigStorage('\t');
