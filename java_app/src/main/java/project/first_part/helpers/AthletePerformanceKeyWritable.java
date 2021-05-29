package project.first_part.helpers;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.*;
import project.utils.Athlete;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
Hadoop like Athlete object, since Hadoop needs special objects that can be used for its Mapper and Reducer classes.
It inherits from the WritableComparable object and implements its basic methods.

As its name suggests, this class is used for the first part of the project, hence it contains 3 attributes,
the id, name and sex of the Athlete.

It also acts as a key for Mapper and Reducer classes, by using the athlete's id as part of the key.
This is achieved by compareTo (which compares Key of the mapper and reducer in hadoop), equals (key equality)
hashCode (used for deciding the partition of the object) methods.
 */
@Data
@Accessors(chain = true)
public class AthletePerformanceKeyWritable implements WritableComparable<AthletePerformanceKeyWritable> {
    private Text name, sex;
    private LongWritable id;
    //default constructor for (de)serialization
    public AthletePerformanceKeyWritable() {
        this.id = new LongWritable();
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
        return this.id.compareTo(o.id);
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
        return (this.id == null) ? 0 : this.id.hashCode();
    }

    // Hadoop will use this method (if available) as a data output in files.
    @Override
    public String toString() {
        return String.format("%s\t%s\t%s", this.id, this.name, this.sex);
    }
}
