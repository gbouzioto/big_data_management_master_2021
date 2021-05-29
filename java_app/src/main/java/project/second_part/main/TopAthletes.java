package project.second_part.main;

import java.io.IOException;
import java.util.*;

import lombok.Builder;
import lombok.Data;
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
import project.second_part.helpers.*;

import static project.utils.Constants.*;

@Builder
@Data
public class TopAthletes {

    public static class MedalMapper
            extends Mapper<LongWritable, Text, TopAthleteKeyWritable, Text>{

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            // split the processed data using tab as a separator
            String[] splitText = line.split("\t");

            // create the Athlete instance
            Athlete athlete = new Athlete(splitText);
            // emit (TopAthleteKeyWritable, 'Gold' || 'Silver' || 'Bronze') based on the medal the athlete has won.
            if (athlete.getMedal().equals(GOLD)) {
                TopAthleteKeyWritable outKey = new TopAthleteKeyWritable(athlete);
                context.write(outKey, GOLD);
            } else if (athlete.getMedal().equals(SILVER)) {
                TopAthleteKeyWritable outKey = new TopAthleteKeyWritable(athlete);
                context.write(outKey, SILVER);
            } else if (athlete.getMedal().equals(BRONZE)) {
                TopAthleteKeyWritable outKey = new TopAthleteKeyWritable(athlete);
                context.write(outKey, BRONZE);
            }
        }
    }

    public static class MedalReducer
            extends Reducer<TopAthleteKeyWritable, Text, IntWritable, TopAthleteKeyWritable> {

        /*
        TreeMap that uses TopAthleteKey instances as keys and a list of TopAthlete instances. The list is used
        since athletes may have the same ranking based on medals. Therefore, chaining is used as a method to handle collisions.
        */
        private TreeMap<AthleteKey, List<TopAthlete>> tMap;

        @Override
        public void setup(Context context) {
            // Initialize the TreeMap in the reducer setup. It will use the TopAthleteKeyComparator to compare AthleteKey keys
            this.tMap = new TreeMap<>(new TopAthleteKeyComparator());
        }

        public void reduce(TopAthleteKeyWritable key, Iterable<Text> values, Context context) {
            int gold = 0, silver = 0, bronze = 0;
            // count the medals of the athlete
            for (Text value : values) {
                if (value.equals(GOLD)) {
                    gold += 1;
                } else if (value.equals(SILVER)) {
                    silver += 1;
                } else if (value.equals(BRONZE)) {
                    bronze += 1;
                }
            }
            // set the gold, silver, bronze and total medals
            key.setGold(gold);
            key.setSilver(silver);
            key.setBronze(bronze);
            key.setTotalMedals();

            // convert the TopAthleteKeyWritable instance to AthleteKey and TopAthlete so they can be written in TreeMap
            AthleteKey athleteKey = new AthleteKey(key);
            TopAthlete topAthlete = new TopAthlete(key);

            List<TopAthlete> topAthleteList;
            // if the key is already in the TreeMap fetch it, otherwise create a new list and put it in the TreeMap
            if (this.tMap.containsKey(athleteKey)) {
                topAthleteList = this.tMap.get(athleteKey);
            } else {
                topAthleteList = new ArrayList<>();
                tMap.put(athleteKey, topAthleteList);
            }
            // add the athlete to the list
            topAthleteList.add(topAthlete);

            // always keep the top 10 entries
            if (this.tMap.size() > MAX_TOP_ATHLETES)
            {
                this.tMap.remove(this.tMap.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException
        {
            // called after the reduce method has finished

            int rank = 1;
            // iterate over the TreeMap entries in descending order
            for (Map.Entry<AthleteKey, List<TopAthlete>> entry : this.tMap.descendingMap().entrySet())
            {
                // retrieve the list of top athletes
                List<TopAthlete> topAthleteList = entry.getValue();
                // if it has more than one athletes, sort them by their names
                if (topAthleteList.size() > 1) {
                    topAthleteList.sort(new TopAthleteNameComparator());
                }
                for(TopAthlete topAthlete:topAthleteList) {
                    // emit(athlete's rank, TopAthletesKeyWritable)
                    context.write(new IntWritable(rank), topAthlete.toTopAthletesKeyWritable());
                }
                rank ++;
            }

        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        // configurations for the Map Reduce Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(TopAthletes.class);

        // Set Mapper, Reducer classes
        job.setMapperClass(MedalMapper.class);
        job.setReducerClass(MedalReducer.class);

        // since we perform ranking only one reducer is used since it needs all available data
        job.setNumReduceTasks(1);

        // Set Output Key Value Classes
        job.setMapOutputKeyClass(TopAthleteKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(TopAthleteKeyWritable.class);

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
        //finding the time difference and converting it into seconds
        float sec = (end - start) / 1000F;
        System.out.println("Time execution in seconds:" + sec);
    }
}