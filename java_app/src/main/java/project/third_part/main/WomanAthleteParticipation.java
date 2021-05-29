package project.third_part.main;

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
import project.third_part.helpers.YearTeamNocAthleteKeyWritable;
import project.third_part.helpers.TeamValue;
import project.third_part.helpers.TeamValueWritable;
import project.third_part.helpers.WomanTeamComparator;

import static project.utils.Constants.*;

@Builder
@Data
public class WomanAthleteParticipation {

    public static class YearTeamMapper
            extends Mapper<LongWritable, Text, YearTeamNocAthleteKeyWritable, YearTeamNocAthleteKeyWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            // split the processed data using tab as a separator
            String[] splitText = line.split("\t");

            // create the Athlete instance
            Athlete athlete = new Athlete(splitText);
            // filter female athletes
            if (athlete.getSex().compareTo(FEMALE) == 0) {
                YearTeamNocAthleteKeyWritable womanAthlete = new YearTeamNocAthleteKeyWritable(athlete);
                // emit(YearTeamNocAthleteKeyWritable, YearTeamNocAthleteKeyWritable) since the same instance can be used as both key and value
                context.write(womanAthlete, womanAthlete);
            }
        }
    }

    public static class YearTeamReducer
            extends Reducer<YearTeamNocAthleteKeyWritable, YearTeamNocAthleteKeyWritable, Text, TeamValueWritable> {

        public void reduce(YearTeamNocAthleteKeyWritable key, Iterable<YearTeamNocAthleteKeyWritable> values, Context context
        ) throws IOException, InterruptedException {
            /*
            Create a HashSet, which will store all unique athlete ids and a HashMap sport -> count , which maps
            the sport and its count, based on the number of athletes who have participated in it.
            */
            HashSet<Long> ids = new HashSet<>();
            HashMap<String, Integer> sportCountMap= new HashMap<>();

            TeamValueWritable teamValueWritable = new TeamValueWritable();
            // set the core attributes of the team
            teamValueWritable.setGames(key.getGames());
            teamValueWritable.setNoc(key.getNoc());
            teamValueWritable.setTeam(key.getTeam());

            String sport;
            long id;

            // count the number of unique athletes and calculate the favorite sport
            for (YearTeamNocAthleteKeyWritable value : values) {
                id = value.getId().get();
                if (ids.contains(id)) {
                    continue;
                }
                ids.add(id);
                sport = value.getSport().toString();
                if (sportCountMap.containsKey(sport)) {
                    sportCountMap.put(sport, sportCountMap.get(sport) + 1);
                } else {
                    sportCountMap.put(sport, 1);
                }

            }
            // set the count of athletes based on the unique number of ids added in the set
            teamValueWritable.setWomanAthletesCount(new IntWritable(ids.size()));

            /*
            TreeMap that uses integers as keys and a list of strings instances. The list is used
            since sports may have the same number of counts. Therefore, chaining is used as a method to handle collisions.
            */
            TreeMap<Integer, List<String>> tMap = new TreeMap<>();
            List<String> favoriteSport;
            // iterate over the entries of the HashMap and insert the count and the list of favorite sports
            for (Map.Entry<String, Integer> entry : sportCountMap.entrySet()) {
                if (tMap.containsKey(entry.getValue())) {
                    favoriteSport = tMap.get(entry.getValue());
                } else {
                    favoriteSport = new ArrayList<>();
                    tMap.put(entry.getValue(), favoriteSport);
                }
                favoriteSport.add(entry.getKey());
            }
            // retrieve the most favorite sport based on the count
            List<String> lastKey = tMap.get(tMap.lastKey());
            // if there is more than one, sort them by character
            if (lastKey.size() > 1) {
                lastKey.sort(Comparator.naturalOrder());
            }
            // because there may be more than one sport, set the favorite sport of the team to be a string of sports separated by space
            teamValueWritable.setSport(new Text(String.join(" ", lastKey)));
            context.write(teamValueWritable.getGames(), teamValueWritable);
        }


    }

    public static class TopYearTeamMapper
            extends Mapper<LongWritable, Text, Text, TeamValueWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            // create a TeamValueWritable instance
            TeamValueWritable teamValueWritable = new TeamValueWritable(value);
            // emit (the games to which the team has participated as a key, TeamValueWritable)
            context.write(teamValueWritable.getGames(), teamValueWritable);
        }
    }

    public static class TopYearTeamReducer
            extends Reducer<Text, TeamValueWritable, Text, TeamValueWritable> {

        public void reduce(Text key, Iterable<TeamValueWritable> values, Context context
        ) throws IOException, InterruptedException {
            /*
            TreeMap that uses Integers as keys and a list of TeamValue instances. The list is used
            since teams may have the same count of women athletes. Therefore, chaining is used as a method to handle collisions.
            */
            TreeMap<Integer, List<TeamValue>> tMap = new TreeMap<>();
            List<TeamValue> teamValueList;

            for (TeamValueWritable value : values) {
                TeamValue teamValue = new TeamValue(value);
                int count = teamValue.getWomanAthletesCount();

                if (tMap.containsKey(count)) {
                    teamValueList = tMap.get(count);
                } else {
                    teamValueList = new ArrayList<>();
                    tMap.put(count, teamValueList);
                }
                teamValueList.add(teamValue);

                // keep the top 3 entries
                if (tMap.size() > MAX_TOP_TEAMS)
                {
                    tMap.remove(tMap.firstKey());
                }
            }

            // iterate over the TreeMap entries in descending order
            for (Map.Entry<Integer, List<TeamValue>> entry : tMap.descendingMap().entrySet())
            {
                // retrieve the list of teams
                List<TeamValue> teamValueList1 = entry.getValue();
                // if it has more than one team, sort them by their team names
                if (teamValueList1.size() > 1) {
                    teamValueList1.sort(new WomanTeamComparator());
                }
                for(TeamValue teamValue:teamValueList1) {
                    TeamValueWritable teamValueWritable = teamValue.toTeamValueWritable();
                    // emit(olympic games year, teamValueWritable)
                    context.write(teamValueWritable.getGames(), teamValueWritable);
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        // configurations for the Map Reduce Jobs
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);

        // Job 1
        Job job1 = Job.getInstance(conf, "woman athlete team participation");
        job1.setJarByClass(WomanAthleteParticipation.class);

        // Set Mapper, Reducer classes
        job1.setMapperClass(YearTeamMapper.class);
        job1.setReducerClass(YearTeamReducer.class);

        // Set Output Key Value Classes
        job1.setMapOutputKeyClass(YearTeamNocAthleteKeyWritable.class);
        job1.setMapOutputValueClass(YearTeamNocAthleteKeyWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(TeamValueWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(out)) {
            fs.delete(out,true);
        }
        // set the input and output files
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2
        Job job2 = Job.getInstance(conf, "top woman athlete team participation");
        job2.setJarByClass(WomanAthleteParticipation.class);

        // Set Mapper, Combiner, Reducer classes
        job2.setMapperClass(TopYearTeamMapper.class);
        job2.setCombinerClass(TopYearTeamReducer.class);
        job2.setReducerClass(TopYearTeamReducer.class);

        // since we perform ranking only one reducer is used since it needs all available data
        job2.setNumReduceTasks(1);

        // Set Output Key Value Classes
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TeamValueWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(TeamValueWritable.class);

        // set the input and output files
        FileInputFormat.addInputPath(job2, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        // finding the time after the operation is executed
        long end = System.currentTimeMillis();
        // finding the time difference and converting it into seconds
        float sec = (end - start) / 1000F;
        System.out.println("Time execution in seconds:" + sec);
    }
}