package project.second_part;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TopAthlete {
    private String name, sex, age, team, sport, games;
    private Integer id, gold, silver, bronze, totalMedals;

    public TopAthlete (TopAthletesKeyWritable topAthletesKeyWritable) {
        this.name = topAthletesKeyWritable.getName().toString();
        this.sex = topAthletesKeyWritable.getSex().toString();
        this.age = topAthletesKeyWritable.getAge().toString();
        this.team = topAthletesKeyWritable.getTeam().toString();
        this.sport = topAthletesKeyWritable.getSport().toString();
        this.games = topAthletesKeyWritable.getGames().toString();
        this.id = topAthletesKeyWritable.getId().get();
        this.gold = topAthletesKeyWritable.getGold().get();
        this.silver = topAthletesKeyWritable.getSilver().get();
        this.bronze = topAthletesKeyWritable.getBronze().get();
        this.totalMedals = topAthletesKeyWritable.getTotalMedals().get();

    }

    public TopAthletesKeyWritable toTopAthletesKeyWritable() {
        return new TopAthletesKeyWritable(this);
    }
}
