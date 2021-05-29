package project.third_part.helpers;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.lang.Integer.parseInt;

/*
Hadoop like Athlete object, since Hadoop needs special objects that can be used for its Mapper and Reducer classes.
It inherits from the WritableComparable object and implements its basic methods.

As its name suggests, this class is used for the third part of the project, hence it contains the attributes:
games, team, noc, sport and womanAthletesCount for a team. This class represents teams that have participated in the
same olympics
 */
@Data
@Accessors(chain = true)
public class TeamValueWritable implements WritableComparable<TeamValueWritable>  {
    private Text games, team, noc, sport;
    private IntWritable womanAthletesCount;
    //default constructor for (de)serialization
    public TeamValueWritable() {
       this.games = new Text();
       this.team = new Text();
       this.noc = new Text();
       this.womanAthletesCount = new IntWritable();
       this.sport = new Text();
    }

    public TeamValueWritable(Text text) {
        String string = text.toString();
        String[] splitText = string.split("\t");
        this.games = new Text(splitText[0]);
        this.team = new Text(splitText[1]);
        this.noc = new Text(splitText[2]);
        this.womanAthletesCount = new IntWritable(parseInt(splitText[3]));
        this.sport = new Text(splitText[4]);

    }

    public void write(DataOutput dataOutput) throws IOException {
        this.team.write(dataOutput);
        this.sport.write(dataOutput);
        this.games.write(dataOutput);
        this.noc.write(dataOutput);
        this.womanAthletesCount.write(dataOutput);

    }
    public void readFields(DataInput dataInput) throws IOException {
        this.team.readFields(dataInput);
        this.sport.readFields(dataInput);
        this.games.readFields(dataInput);
        this.noc.readFields(dataInput);
        this.womanAthletesCount.readFields(dataInput);
    }

    public int compareTo(TeamValueWritable o) {
        return this.games.compareTo(o.games);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TeamValueWritable that = (TeamValueWritable) o;
        return that.games.equals(this.games);
    }

    @Override
    public int hashCode() {
        return (this.games == null) ? 0 : this.games.hashCode();
    }

    // Hadoop will use this method (if available) as a data output in files.
    @Override
    public String toString() {
        return String.format("%s\t%s\t%s\t%s", this.team, this.noc, this.womanAthletesCount, this.sport);
    }
}
