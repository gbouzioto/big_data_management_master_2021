package project.third_part.helpers;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.*;
import project.utils.Athlete;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@Accessors(chain = true)
public class SameYearTeamAthleteKeyWritable implements WritableComparable<SameYearTeamAthleteKeyWritable> {
    protected Text name, team, sport, games, noc;
    protected LongWritable id;
    //default constructor for (de)serialization
    public SameYearTeamAthleteKeyWritable() {
        this.id = new LongWritable();
        this.name = new Text();
        this.team = new Text();
        this.sport = new Text();
        this.games = new Text();
        this.noc = new Text();
    }
    public SameYearTeamAthleteKeyWritable(Athlete athlete) {
        this.id = athlete.getId();
        this.name = athlete.getName();
        this.team = athlete.getTeam();
        this.sport = athlete.getSport();
        this.games = athlete.getGames();
        this.noc = athlete.getNoc();
    }


    public void write(DataOutput dataOutput) throws IOException {
        this.id.write(dataOutput);
        this.name.write(dataOutput);
        this.team.write(dataOutput);
        this.sport.write(dataOutput);
        this.games.write(dataOutput);
        this.noc.write(dataOutput);

    }
    public void readFields(DataInput dataInput) throws IOException {
        this.id.readFields(dataInput);
        this.name.readFields(dataInput);
        this.team.readFields(dataInput);
        this.sport.readFields(dataInput);
        this.games.readFields(dataInput);
        this.noc.readFields(dataInput);
    }

    public int compareTo(SameYearTeamAthleteKeyWritable o) {
        int result = this.games.compareTo(o.games);
        if (result == 0) {
            result = this.team.compareTo(o.team);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SameYearTeamAthleteKeyWritable that = (SameYearTeamAthleteKeyWritable) o;
        return that.games.equals(this.games) && that.team.equals(this.team);
    }

    @Override
    public int hashCode() {
        int result = ((this.team == null) ? 0 : this.team.hashCode());
        result += ((this.games == null) ? 0 : this.games.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s %s", this.name, this.team, this.sport, this.games);
    }
}
