package project.second_part;

import java.io.IOException;
import java.io.StringReader;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import project.Athlete;

import static project.Constants.*;


public class TopAthletes {

    public static class MedalMapper
            extends Mapper<LongWritable, Text, TopAthletesKeyWritable, Text>{

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            // skip header
            if (key.get() == 0) {
                return;
            }

            String line = value.toString();

            try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
                String[] parsedLine = csvReader.readNext();
                Athlete athlete = new Athlete(parsedLine);
                if (athlete.getMedal().equals(GOLD)) {
                    TopAthletesKeyWritable outKey = new TopAthletesKeyWritable(athlete);
                    context.write(outKey, GOLD);
                } else if (athlete.getMedal().equals(SILVER)) {
                    TopAthletesKeyWritable outKey = new TopAthletesKeyWritable(athlete);
                    context.write(outKey, SILVER);
                } else if (athlete.getMedal().equals(BRONZE)) {
                    TopAthletesKeyWritable outKey = new TopAthletesKeyWritable(athlete);
                    context.write(outKey, BRONZE);
                }
            } catch (CsvValidationException e) {
                e.printStackTrace();
            }
        }
    }

    public static class MedalReducer
            extends Reducer<TopAthletesKeyWritable, Text, TopAthletesKeyWritable, IntWritable> {

        public void reduce(TopAthletesKeyWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int gold = 0, silver = 0, bronze = 0;
            for (Text value : values) {
                if (value.equals(GOLD)) {
                    gold += 1;
                } else if (value.equals(SILVER)) {
                    silver += 1;
                } else if (value.equals(BRONZE)) {
                    bronze += 1;
                }
            }
            key.setGold(gold);
            key.setSilver(silver);
            key.setBronze(bronze);
            key.setTotalMedals();
            context.write(key, new IntWritable(1));
        }
    }

    public static class TopAthletesComparator extends WritableComparator {

        protected TopAthletesComparator() {
            super(TopAthletesKeyWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TopAthletesKeyWritable at1 = (TopAthletesKeyWritable) w1;
            TopAthletesKeyWritable at2 = (TopAthletesKeyWritable) w2;

            int result = -1* at1.getGold().compareTo(at2.getGold());
            if (result == 0) {
                result = -1* at1.getTotalMedals().compareTo(at2.getTotalMedals());
            }
            if (result == 0) {
                return at1.getName().compareTo(at2.getName());
            }
            return result;
        }
    }

    public static class TopAthletesGroupingComparator extends WritableComparator {

        protected TopAthletesGroupingComparator() {
            super(TopAthletesKeyWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TopAthletesKeyWritable at1 = (TopAthletesKeyWritable) w1;
            TopAthletesKeyWritable at2 = (TopAthletesKeyWritable) w2;
            int result = at1.getId().compareTo(at2.getId());
            if (result == 0) {
                result = at1.getGames().compareTo(at2.getGames());
            }
            if (at1.getName().compareTo(new Text("Michael Fred Phelps, II")) == 0) {
                int foo =1;
            }
            return result;
        }
    }

    public static class TopAthletesKeyPartitioner extends Partitioner<TopAthletesKeyWritable, Text> {

        @Override
        public int getPartition(TopAthletesKeyWritable key, Text val, int numPartitions) {
            int hash = key.hashCode();
            return hash % numPartitions;
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(TopAthletes.class);

        job.setGroupingComparatorClass(TopAthletesGroupingComparator.class);
        job.setSortComparatorClass(TopAthletesComparator.class);
        job.setPartitionerClass(TopAthletesKeyPartitioner.class);

        job.setMapperClass(MedalMapper.class);
        job.setReducerClass(MedalReducer.class);

        job.setMapOutputKeyClass(TopAthletesKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(TopAthletesKeyWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]),true);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}