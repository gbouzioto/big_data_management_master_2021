package project.second_part.helpers;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.*;
import project.utils.Athlete;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@Accessors(chain = true)
public class TopAthleteKeyWritable implements WritableComparable<TopAthleteKeyWritable> {
    private Text name, sex, age, team, sport, games;
    private IntWritable gold, silver, bronze, totalMedals;
    private LongWritable id;
    //default constructor for (de)serialization
    public TopAthleteKeyWritable() {
        this.id = new LongWritable();
        this.name = new Text();
        this.sex = new Text();
        this.age = new Text();
        this.team = new Text();
        this.sport = new Text();
        this.games = new Text();
        this.gold = new IntWritable();
        this.silver = new IntWritable();
        this.bronze = new IntWritable();
        this.totalMedals = new IntWritable();
    }
    public TopAthleteKeyWritable(Athlete athlete) {
        this.id = athlete.getId();
        this.name = athlete.getName();
        this.sex = athlete.getSex();
        this.age = athlete.getAge();
        this.team = athlete.getTeam();
        this.sport = athlete.getSport();
        this.games = athlete.getGames();
        this.gold = new IntWritable();
        this.silver = new IntWritable();
        this.bronze = new IntWritable();
        this.totalMedals = new IntWritable();
    }

    public TopAthleteKeyWritable(TopAthlete topAthlete) {
        this.id = new LongWritable(topAthlete.getId());
        this.name = new Text(topAthlete.getName());
        this.sex = new Text(topAthlete.getSex());
        this.age = new Text(topAthlete.getAge());
        this.team = new Text(topAthlete.getTeam());
        this.sport = new Text(topAthlete.getSport());
        this.games = new Text(topAthlete.getGames());
        this.gold = new IntWritable(topAthlete.getGold());
        this.silver = new IntWritable(topAthlete.getSilver());
        this.bronze = new IntWritable(topAthlete.getBronze());
        this.totalMedals = new IntWritable(topAthlete.getTotalMedals());
    }
    
    public void write(DataOutput dataOutput) throws IOException {
        this.id.write(dataOutput);
        this.name.write(dataOutput);
        this.sex.write(dataOutput);
        this.age.write(dataOutput);
        this.team.write(dataOutput);
        this.sport.write(dataOutput);
        this.games.write(dataOutput);
        this.gold.write(dataOutput);
        this.silver.write(dataOutput);
        this.bronze.write(dataOutput);
        this.totalMedals.write(dataOutput);
    }
    public void readFields(DataInput dataInput) throws IOException {
        this.id.readFields(dataInput);
        this.name.readFields(dataInput);
        this.sex.readFields(dataInput);
        this.age.readFields(dataInput);
        this.team.readFields(dataInput);
        this.sport.readFields(dataInput);
        this.games.readFields(dataInput);
        this.gold.readFields(dataInput);
        this.silver.readFields(dataInput);
        this.bronze.readFields(dataInput);
        this.totalMedals.readFields(dataInput);
    }

    public int compareTo(TopAthleteKeyWritable o) {
        int result = this.id.compareTo(o.id);
        if (result == 0) {
            result = this.games.compareTo(o.games);
        }
        return result;
    }

    public void setGold(int value) {
        this.gold.set(value);
    }

    public void setSilver(int value) {
        this.silver.set(value);
    }

    public void setBronze(int value) {
        this.bronze.set(value);
    }

    public void setTotalMedals() {
        int total = this.gold.get() + this.silver.get() + this.bronze.get();
        this.totalMedals.set(total);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopAthleteKeyWritable that = (TopAthleteKeyWritable) o;
        return that.id.equals(this.id) && that.games.equals(this.games);
    }

    @Override
    public int hashCode() {
        int result = ((this.id == null) ? 0 : this.id.hashCode());
        result += ((this.games == null) ? 0 : this.games.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
                this.name, this.sex, this.age, this.team, this.sport,
                this.games, this.gold, this.silver, this.bronze, this.totalMedals);
    }
}
