package project.second_part;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.TreeMap;

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
            extends Reducer<TopAthletesKeyWritable, Text, IntWritable, TopAthletesKeyWritable> {

        private TreeMap<AthleteKey, TopAthlete> tmap2;

        @Override
        public void setup(Context context) {
            this.tmap2 = new TreeMap<>(new TopAthleteKeyComparator());
        }

        public void reduce(TopAthletesKeyWritable key, Iterable<Text> values, Context context) {
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

            tmap2.put(athleteKey, topAthlete);

            if (this.tmap2.size() > 30)
            {
                this.tmap2.remove(this.tmap2.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException
        {

            // write top athlete of all times
            AthleteKey bestAthleteKey = this.tmap2.lastKey();
            TopAthlete bestTopAthlete = this.tmap2.get(bestAthleteKey);
            context.write(new IntWritable(1), bestTopAthlete.toTopAthletesKeyWritable());

            int count = 1;
            // iterate from the second athlete onward
            for (Map.Entry<AthleteKey, TopAthlete> entry : this.tmap2.descendingMap().subMap(bestAthleteKey,
                    false, this.tmap2.descendingMap().lastKey(), true).entrySet())
            {
                // previous
                Map.Entry<AthleteKey, TopAthlete> prev = tmap2.descendingMap().lowerEntry(entry.getKey());
                AthleteKey prevAthleteKey = prev.getKey();
                // current
                AthleteKey AthleteKey = entry.getKey();
                TopAthlete topAthlete = entry.getValue();

                if (!AthleteKey.equals(prevAthleteKey)) {
                    count ++;
                }
                context.write(new IntWritable(count), topAthlete.toTopAthletesKeyWritable());
                if (count == 10) {
                    //check next athlete before exiting
                    Map.Entry<AthleteKey, TopAthlete> next = tmap2.descendingMap().higherEntry(entry.getKey());  // next
                    if (next == null) {
                        break;
                    }
                    AthleteKey nextAthleteKey = next.getKey();

                    if (!AthleteKey.equals(nextAthleteKey)) {
                        count ++;
                    }
                }
                if (count > 10) {
                    break;
                }
            }

        }
    }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(TopAthletes.class);

        job.setMapperClass(MedalMapper.class);
        job.setReducerClass(MedalReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(TopAthletesKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(TopAthletesKeyWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]),true);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}