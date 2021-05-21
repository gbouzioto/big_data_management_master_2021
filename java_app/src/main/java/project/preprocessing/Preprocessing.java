package project.preprocessing;

import java.io.*;
import java.util.*;

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



public class Preprocessing {

    private static List<HashMap<String, String>> nocMapping;

    public static class PreprocessingMapper
            extends Mapper<LongWritable, Text, LongWritable, Text>{

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            // skip header
            if (key.get() == 0) {
                return;
            }

            String line = value.toString();
            HashMap<String, String> mNOC= nocMapping.get(0);
            HashMap<String, String> mNotes= nocMapping.get(1);

            try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
                String[] parsedLine = csvReader.readNext();
                Athlete athlete = new Athlete(parsedLine);
                String noc = athlete.getNoc().toString();
                String region = mNOC.get(noc);
                String true_noc;
                if (region == null) {
                    true_noc = mNotes.get(noc);
                    region = mNOC.get(true_noc);
                }
                athlete.setTeam(new Text(region));
                context.write(key, athlete.toText());

            } catch (CsvValidationException e) {
                e.printStackTrace();
            }
        }
    }

    public static class PreprocessingReducer
            extends Reducer<LongWritable, Text, LongWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void setNOCMapping (String fileName) {
        List<HashMap<String, String>> result = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(fileName))) {
            String[] parsedLine;
            // skip header
            reader.readNext();
            HashMap<String, String> mNOC= new HashMap<>();
            HashMap<String, String> mNotes= new HashMap<>();
            while ((parsedLine = reader.readNext()) != null) {
                String noc = parsedLine[0];
                String region = parsedLine[1];
                String notes = parsedLine[2];
                mNOC.put(noc, region);
                if (notes != null) {
                    mNotes.put(notes, noc);
                }
            }
            result.add(mNOC);
            result.add(mNotes);
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
        nocMapping = result;
    }




    public static void main(String[] args) throws Exception {
        setNOCMapping(args[0]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "athlete performance");
        job.setJarByClass(Preprocessing.class);

        job.setMapperClass(PreprocessingMapper.class);
        job.setCombinerClass(PreprocessingReducer.class);
        job.setReducerClass(PreprocessingReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]),true);
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}