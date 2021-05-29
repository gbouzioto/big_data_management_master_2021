package project.second_part.helpers;

import lombok.Data;
import lombok.experimental.Accessors;

/*
Converts a TopAthleteKeyWritable to a TopAthlete in order to be used in intermediate data structures.
Hadoop objects are optimized in way such that their bytes cannot be written more than once in other data structures.
Therefore, this conversion is used.
 */
@Data
@Accessors(chain = true)
public class TopAthlete {
    private String name, sex, age, team, sport, games;
    private Integer gold, silver, bronze, totalMedals;
    private Long id;

    public TopAthlete (TopAthleteKeyWritable topAthleteKeyWritable) {
        this.name = topAthleteKeyWritable.getName().toString();
        this.sex = topAthleteKeyWritable.getSex().toString();
        this.age = topAthleteKeyWritable.getAge().toString();
        this.team = topAthleteKeyWritable.getTeam().toString();
        this.sport = topAthleteKeyWritable.getSport().toString();
        this.games = topAthleteKeyWritable.getGames().toString();
        this.id = topAthleteKeyWritable.getId().get();
        this.gold = topAthleteKeyWritable.getGold().get();
        this.silver = topAthleteKeyWritable.getSilver().get();
        this.bronze = topAthleteKeyWritable.getBronze().get();
        this.totalMedals = topAthleteKeyWritable.getTotalMedals().get();

    }

    public TopAthleteKeyWritable toTopAthletesKeyWritable() {
        return new TopAthleteKeyWritable(this);
    }
}
