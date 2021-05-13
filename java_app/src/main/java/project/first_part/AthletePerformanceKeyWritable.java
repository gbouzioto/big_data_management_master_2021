package project.first_part;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.*;
import project.Athlete;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@Data
@Accessors(chain = true)
public class AthletePerformanceKeyWritable implements WritableComparable<AthletePerformanceKeyWritable> {
    private Text name, sex;
    private IntWritable id;
    //default constructor for (de)serialization
    public AthletePerformanceKeyWritable() {
        id = new IntWritable();
        name = new Text();
        sex = new Text();
    }
    public AthletePerformanceKeyWritable(Athlete athlete) {
        this.id = athlete.getId();
        this.name = athlete.getName();
        this.sex = athlete.getSex();
    }
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        name.write(dataOutput);
        sex.write(dataOutput);
    }
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        name.readFields(dataInput);
        sex.readFields(dataInput);
    }

    public int compareTo(AthletePerformanceKeyWritable o) {
        return this.id.compareTo(o.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AthletePerformanceKeyWritable that = (AthletePerformanceKeyWritable) o;
        return Objects.equals(id, that.id);
    }
    @Override
    public int hashCode() {
        return Objects.hash(id, name, sex);
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", id, name, sex);
    }
}
