package project.second_part;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.*;
import project.Athlete;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@Accessors(chain = true)
public class TopAthletesKeyWritable implements WritableComparable<TopAthletesKeyWritable> {
    private Text name, sex, age, team, sport, games;
    private IntWritable id, gold, silver, bronze, totalMedals;
    //default constructor for (de)serialization
    public TopAthletesKeyWritable() {
        this.id = new IntWritable();
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
    public TopAthletesKeyWritable(Athlete athlete) {
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

    public TopAthletesKeyWritable(TopAthlete topAthlete) {
        this.id = new IntWritable(topAthlete.getId());
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

    public int compareTo(TopAthletesKeyWritable o) {
        int result = this.getId().compareTo(o.getId());
        if (result == 0) {
            result = this.getGames().compareTo(o.getGames());
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
        TopAthletesKeyWritable that = (TopAthletesKeyWritable) o;
        return that.getId().equals(this.id) && that.getGames().equals(this.games);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.id == null) ? 0 : this.id.hashCode());
        result = prime * result + ((this.games == null) ? 0 : this.games.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s %s %s %s %s %s %s %s",
                this.name, this.sex, this.age, this.team, this.sport,
                this.games, this.gold, this.silver, this.bronze, this.totalMedals);
    }
}
