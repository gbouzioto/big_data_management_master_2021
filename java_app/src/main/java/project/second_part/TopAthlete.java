package project.second_part;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TopAthlete {
    private String name, sex, age, team, sport, games;
    private Integer id, gold, silver, bronze, totalMedals;

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
