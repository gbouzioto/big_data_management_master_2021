package project.second_part;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
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
import project.Athlete;

import static project.Constants.*;

@Builder
@Data
public class TopAthletes {

    public static class MedalMapper
            extends Mapper<LongWritable, Text, TopAthleteKeyWritable, Text>{

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
                    TopAthleteKeyWritable outKey = new TopAthleteKeyWritable(athlete);
                    context.write(outKey, GOLD);
                } else if (athlete.getMedal().equals(SILVER)) {
                    TopAthleteKeyWritable outKey = new TopAthleteKeyWritable(athlete);
                    context.write(outKey, SILVER);
                } else if (athlete.getMedal().equals(BRONZE)) {
                    TopAthleteKeyWritable outKey = new TopAthleteKeyWritable(athlete);
                    context.write(outKey, BRONZE);
                }
            } catch (CsvValidationException e) {
                e.printStackTrace();
            }
        }
    }

    public static class MedalReducer
            extends Reducer<TopAthleteKeyWritable, Text, IntWritable, TopAthleteKeyWritable> {

        private TreeMap<AthleteKey, List<TopAthlete>> tMap;

        @Override
        public void setup(Context context) {
            this.tMap = new TreeMap<>(new TopAthleteKeyComparator());
        }

        public void reduce(TopAthleteKeyWritable key, Iterable<Text> values, Context context) {
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

            AthleteKey athleteKey = new AthleteKey(key);
            TopAthlete topAthlete = new TopAthlete(key);

            List<TopAthlete> topAthleteList;
            if (this.tMap.containsKey(athleteKey)) {
                topAthleteList = this.tMap.get(athleteKey);
            } else {
                topAthleteList = new ArrayList<>();
            }
            topAthleteList.add(topAthlete);
            tMap.put(athleteKey, topAthleteList);

            if (this.tMap.size() > MAX_TOP_ATHLETES)
            {
                this.tMap.remove(this.tMap.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException
        {
            int count = 1;
            for (Map.Entry<AthleteKey, List<TopAthlete>> entry : this.tMap.descendingMap().entrySet())
            {
                List<TopAthlete> topAthleteList = entry.getValue();
                if (topAthleteList.size() > 1) {
                    topAthleteList.sort(new TopAthleteNameComparator());
                }
                for(TopAthlete topAthlete:topAthleteList) {
                    context.write(new IntWritable(count), topAthlete.toTopAthletesKeyWritable());
                }
                count ++;
            }

        }
    }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(TopAthletes.class);

        job.setMapperClass(MedalMapper.class);
        job.setReducerClass(MedalReducer.class);

        job.setMapOutputKeyClass(TopAthleteKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(TopAthleteKeyWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]),true);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}