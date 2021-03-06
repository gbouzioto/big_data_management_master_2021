package project.third_part.helpers;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/*
Converts a TeamValueWritable to a TeamValue in order to be used in intermediate data structures.
Hadoop objects are optimized in way such that their bytes cannot be written more than once in other data structures.
Therefore, this conversion is used.
 */
@Data
@Accessors(chain = true)
public class TeamValue {
    private String games, team, noc, sport;
    private Integer womanAthletesCount;

    public TeamValue(TeamValueWritable teamValueWritable) {
        this.games = teamValueWritable.getGames().toString();
        this.team = teamValueWritable.getTeam().toString();
        this.noc = teamValueWritable.getNoc().toString();
        this.womanAthletesCount = teamValueWritable.getWomanAthletesCount().get();
        this.sport = teamValueWritable.getSport().toString();

    }

    public TeamValueWritable toTeamValueWritable() {
        TeamValueWritable teamValueWritable = new TeamValueWritable();

        teamValueWritable.setGames(new Text(this.games));
        teamValueWritable.setTeam(new Text(this.team));
        teamValueWritable.setNoc(new Text(this.noc));
        teamValueWritable.setWomanAthletesCount(new IntWritable(this.womanAthletesCount));
        teamValueWritable.setSport(new Text(this.sport));
        return teamValueWritable;
    }
}
