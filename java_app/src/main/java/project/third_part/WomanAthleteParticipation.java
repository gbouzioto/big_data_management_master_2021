package project.third_part;

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
import project.Athlete;

import static project.Constants.*;

@Builder
@Data
public class WomanAthleteParticipation {

    public static class YearTeamMapper
            extends Mapper<LongWritable, Text, SameYearTeamAthleteKeyWritable, SameYearTeamAthleteKeyWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splitText = line.split("\t");

            Athlete athlete = new Athlete(splitText);
            if (athlete.getSex().compareTo(FEMALE) == 0) {
                SameYearTeamAthleteKeyWritable womanAthlete = new SameYearTeamAthleteKeyWritable(athlete);
                context.write(womanAthlete, womanAthlete);
            }
        }
    }

    public static class YearTeamReducer
            extends Reducer<SameYearTeamAthleteKeyWritable, SameYearTeamAthleteKeyWritable, Text, TeamValueWritable> {

        public void reduce(SameYearTeamAthleteKeyWritable key, Iterable<SameYearTeamAthleteKeyWritable> values, Context context
        ) throws IOException, InterruptedException {
            HashSet<Long> ids = new HashSet<>();
            HashMap<String, Integer> sportCountMap= new HashMap<>();

            TeamValueWritable teamValueWritable = new TeamValueWritable();
            // set prime attributes
            teamValueWritable.setGames(key.getGames());
            teamValueWritable.setNoc(key.getNoc());
            teamValueWritable.setTeam(key.getTeam());

            String sport;
            long id;

            for (SameYearTeamAthleteKeyWritable value : values) {
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
            teamValueWritable.setWomanAthletesCount(new IntWritable(ids.size()));
            String favoriteSport = "";
            int max_count = 0;
            for (Map.Entry<String, Integer> entry : sportCountMap.entrySet()) {
                if (entry.getValue() > max_count) {
                    favoriteSport = entry.getKey();
                    max_count = entry.getValue();
                }
            }
            teamValueWritable.setSport(new Text(favoriteSport));
            context.write(teamValueWritable.getGames(), teamValueWritable);
        }


    }

    public static class TopYearTeamMapper
            extends Mapper<LongWritable, Text, Text, TeamValueWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            TeamValueWritable teamValueWritable = new TeamValueWritable(value);
            context.write(teamValueWritable.getGames(), teamValueWritable);
        }
    }

    public static class TopYearTeamReducer
            extends Reducer<Text, TeamValueWritable, Text, TeamValueWritable> {

        public void reduce(Text key, Iterable<TeamValueWritable> values, Context context
        ) throws IOException, InterruptedException {
            TreeMap<Integer, List<TeamValue>> tMap = new TreeMap<>();
            List<TeamValue> teamValueList;

            for (TeamValueWritable value : values) {
                TeamValue teamValue = new TeamValue(value);
                int count = teamValue.getWomanAthletesCount();

                if (tMap.containsKey(count)) {
                    teamValueList = tMap.get(count);
                } else {
                    teamValueList = new ArrayList<>();
                }
                teamValueList.add(teamValue);
                tMap.put(count, teamValueList);

                if (tMap.size() > MAX_TOP_TEAMS)
                {
                    tMap.remove(tMap.firstKey());
                }
            }

            for (Map.Entry<Integer, List<TeamValue>> entry : tMap.descendingMap().entrySet())
            {
                List<TeamValue> teamValueList1 = entry.getValue();
                if (teamValueList1.size() > 1) {
                    teamValueList1.sort(new WomanTeamComparator());
                }
                for(TeamValue teamValue:teamValueList1) {
                    TeamValueWritable teamValueWritable = teamValue.toTeamValueWritable();
                    context.write(teamValueWritable.getGames(), teamValueWritable);
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);

        Job job1 = Job.getInstance(conf, "woman athlete team participation");
        job1.setJarByClass(WomanAthleteParticipation.class);

        job1.setMapperClass(YearTeamMapper.class);
        job1.setReducerClass(YearTeamReducer.class);

        job1.setMapOutputKeyClass(SameYearTeamAthleteKeyWritable.class);
        job1.setMapOutputValueClass(SameYearTeamAthleteKeyWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(TeamValueWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(out)) {
            fs.delete(out,true);
        }
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "top woman athlete team participation");
        job2.setJarByClass(WomanAthleteParticipation.class);

        job2.setMapperClass(TopYearTeamMapper.class);
        job2.setCombinerClass(TopYearTeamReducer.class);
        job2.setReducerClass(TopYearTeamReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TeamValueWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(TeamValueWritable.class);

        FileInputFormat.addInputPath(job2, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}