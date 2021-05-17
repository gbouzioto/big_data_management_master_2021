package project.first_part;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import project.Athlete;

import static project.Constants.GOLD;


public class AthletePerformance {

    public static class GoldMedalMapper
            extends Mapper<LongWritable, Text, AthletePerformanceKeyWritable, IntWritable>{

        private final static IntWritable ONE = new IntWritable(1);

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
                    AthletePerformanceKeyWritable outKey = new AthletePerformanceKeyWritable(athlete);
                    context.write(outKey, ONE);
                }
            } catch (CsvValidationException e) {
                e.printStackTrace();
            }
        }
    }

    public static class GoldMedalReducer
            extends Reducer<AthletePerformanceKeyWritable, IntWritable, AthletePerformanceKeyWritable, IntWritable> {

        public void reduce(AthletePerformanceKeyWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class AthletePerformanceComparator extends WritableComparator {

        protected AthletePerformanceComparator() {
            super(AthletePerformanceKeyWritable.class, true);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(AthletePerformance.class);
        job.setMapperClass(GoldMedalMapper.class);
        job.setCombinerClass(GoldMedalReducer.class);
        job.setReducerClass(GoldMedalReducer.class);
        job.setMapOutputKeyClass(AthletePerformanceKeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(AthletePerformanceKeyWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(AthletePerformanceComparator.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]),true);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}