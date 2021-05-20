package project.third_part;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.lang.Integer.parseInt;

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
        int result = this.games.compareTo(o.games);
        if (result == 0) {
            result = this.team.compareTo(o.team);
        }
        if (result == 0) {
            result = this.noc.compareTo(o.noc);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TeamValueWritable that = (TeamValueWritable) o;
        return that.games.equals(this.games) && that.team.equals(this.team) && that.noc.equals(this.noc);
    }

    @Override
    public int hashCode() {
        int result = ((this.team == null) ? 0 : this.team.hashCode());
        result += ((this.games == null) ? 0 : this.games.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s\t%s", this.team, this.noc, this.womanAthletesCount, this.sport);
    }
}
