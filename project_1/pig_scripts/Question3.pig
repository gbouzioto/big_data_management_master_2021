-- Define functions
REGISTER 'piggybank-0.17.0.jar';
DEFINE Stitch org.apache.pig.piggybank.evaluation.Stitch;
DEFINE IOver org.apache.pig.piggybank.evaluation.Over('int');

-- Load data
data = LOAD 'athlete_clean' USING PigStorage('\t') AS (id:long, name:chararray, sex:chararray, age:int, height:int, weight:int, team:chararray, noc:chararray, games:chararray, year:int, season:chararray, city:chararray, sport:chararray, event:chararray, medal:chararray);

-- Keep only female athletes
female_data = FILTER data BY sex == 'F';

-- Keep only relevant data
relevant_data = FOREACH female_data GENERATE games, team, noc, sport, id;

-- Group data to find the team/sport with the most female athletes
grouped_data = GROUP relevant_data BY (games, team, noc);
grouped_sport = GROUP relevant_data BY (games, team, noc, sport);

-- Generate athlete and sport counts
athlete_count = FOREACH grouped_data {
		unique_id = DISTINCT relevant_data.id;
		GENERATE group.games, group.team, group.noc,
		COUNT(unique_id) as entries;
		};

sport_count = FOREACH grouped_sport {
		unique_id = DISTINCT relevant_data.id;
		GENERATE group.games, group.team, group.noc,
		group.sport, COUNT(unique_id) as entries;
		};

-- Get the top 3 teams/top 1 sport with the most female athletes
grouped_athlete_count = GROUP athlete_count BY games;
grouped_sport_count = GROUP sport_count BY (games, team, noc);

-- Rank results based on athlete/sport count
rank_athlete_count = FOREACH grouped_athlete_count {
			ordered = ORDER athlete_count BY entries DESC;
			GENERATE FLATTEN(Stitch(ordered, IOver(ordered,
			'dense_rank', -1, -1, 3)));
			};

rank_sport_count = FOREACH grouped_sport_count {
			ordered = ORDER sport_count BY entries DESC;
			GENERATE FLATTEN(Stitch(ordered, IOver(ordered,
			'dense_rank', -1, -1, 4)));
			};

-- Keep top 3 teams/top 1 sport (or more if equal)
top_athlete_count = FILTER rank_athlete_count BY stitched::result <= 3;
top_sport_count = FILTER rank_sport_count BY stitched::result == 1;

-- Generate and combine results
final_athlete_count = FOREACH top_athlete_count GENERATE
			stitched::games AS games, stitched::team AS team,
			stitched::noc AS noc, stitched::entries AS entries;

final_sport_count = FOREACH top_sport_count GENERATE
			stitched::games AS games, stitched::team AS team,
			stitched::noc AS noc, stitched::sport AS sport;

combined_result = JOIN final_athlete_count BY (games, team, noc),
		final_sport_count BY (games, team, noc);

final_result = FOREACH combined_result GENERATE
			final_athlete_count::games AS games,
			final_athlete_count::team AS team, 
			final_athlete_count::noc AS noc,
			final_athlete_count::entries AS entries,
			final_sport_count::sport AS sport;

-- Show both preferred sports
grouped_final_result = GROUP final_result BY (games, team, noc);

flat_final_result = FOREACH grouped_final_result GENERATE group.games, group.team, group.noc, FLATTEN(final_result.entries) as entries, FLATTEN(BagToTuple(final_result.sport)) as sport;

unique_final_result = DISTINCT flat_final_result;

-- Order results
final_result = ORDER unique_final_result BY games ASC, entries DESC, team ASC;

-- Save results
STORE final_result INTO 'question3' USING PigStorage('\t');
