package project;

import java.io.IOException;
import java.io.StringReader;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AthletePerformance {

    public static class GoldMedalMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final Text athleteMeta = new Text();
        private final Text medal = new Text();
        private final static Text gold = new Text("Gold");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();

            try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
                String[] ParsedLine = csvReader.readNext();
                String meta = String.format("%s %s %s",
                        ParsedLine[Constants.ID],
                        ParsedLine[Constants.NAME],
                        ParsedLine[Constants.SEX]);
                medal.set(ParsedLine[Constants.MEDAL]);
                athleteMeta.set(meta);
            } catch (CsvValidationException e) {
                e.printStackTrace();
            }
            if (medal.equals(gold)) {
                context.write(athleteMeta, one);
            }
        }
    }

    public static class GoldMedalReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(AthletePerformance.class);
        job.setMapperClass(GoldMedalMapper.class);
        job.setCombinerClass(GoldMedalReducer.class);
        job.setReducerClass(GoldMedalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}