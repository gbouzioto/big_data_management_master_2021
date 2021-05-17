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
        this.id = new IntWritable();
        this.name = new Text();
        this.sex = new Text();
    }
    public AthletePerformanceKeyWritable(Athlete athlete) {
        this.id = athlete.getId();
        this.name = athlete.getName();
        this.sex = athlete.getSex();
    }
    public void write(DataOutput dataOutput) throws IOException {
        this.id.write(dataOutput);
        this.name.write(dataOutput);
        this.sex.write(dataOutput);
    }
    public void readFields(DataInput dataInput) throws IOException {
        this.id.readFields(dataInput);
        this.name.readFields(dataInput);
        this.sex.readFields(dataInput);
    }

    public int compareTo(AthletePerformanceKeyWritable o) {
        return this.id.compareTo(o.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AthletePerformanceKeyWritable that = (AthletePerformanceKeyWritable) o;
        return Objects.equals(this.id, that.id);
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.id == null) ? 0 : this.id.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", this.id, this.name, this.sex);
    }
}
