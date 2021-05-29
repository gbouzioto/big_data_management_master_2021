package project.first_part.main;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import project.utils.Athlete;
import project.first_part.helpers.AthletePerformanceKeyWritable;

import static project.utils.Constants.GOLD;
import static project.utils.Constants.ONE;


public class AthletePerformance {

    public static class GoldMedalMapper
            extends Mapper<LongWritable, Text, AthletePerformanceKeyWritable, IntWritable>{

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            // split the processed data using tab as a separator
            String[] splitText = line.split("\t");

            // create the Athlete instance
            Athlete athlete = new Athlete(splitText);
            if (athlete.getMedal().equals(GOLD)) {
                // if the athlete has a gold medal then emit (AthletePerformanceKeyWritable, 1)
                AthletePerformanceKeyWritable outKey = new AthletePerformanceKeyWritable(athlete);
                context.write(outKey, ONE);
            }
        }
    }

    public static class GoldMedalReducer
            extends Reducer<AthletePerformanceKeyWritable, IntWritable, AthletePerformanceKeyWritable, IntWritable> {

        public void reduce(AthletePerformanceKeyWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // gather the gold medals for each athlete
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            // emit (AthletePerformanceKeyWritable, sum of gold medals)
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        // configurations for the Map Reduce Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(AthletePerformance.class);

        // Set Mapper, Combiner, Reducer classes
        job.setMapperClass(GoldMedalMapper.class);
        job.setCombinerClass(GoldMedalReducer.class);
        job.setReducerClass(GoldMedalReducer.class);

        // Set Output Key Value Classes
        job.setMapOutputKeyClass(AthletePerformanceKeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(AthletePerformanceKeyWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]),true);
        }
        // set the input and output files
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
        // finding the time after the operation is executed
        long end = System.currentTimeMillis();
        // finding the time difference and converting it into seconds
        float sec = (end - start) / 1000F;
        System.out.println("Time execution in seconds:" + sec);
    }
}