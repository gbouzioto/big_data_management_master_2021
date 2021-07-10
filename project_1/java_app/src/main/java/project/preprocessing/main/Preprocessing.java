package project.preprocessing.main;

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
import project.utils.Athlete;



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
                // get region from the noc mapping
                String region = mNOC.get(noc);
                String true_noc;
                if (region == null) {
                    // if region is null, get the region from the notes, since the country has changed
                    true_noc = mNotes.get(noc);
                    region = mNOC.get(true_noc);
                }
                // set the team of the athlete to be the same as the region
                athlete.setTeam(new Text(region));
                // emit the athlete key as long as all other attributes
                context.write(athlete.getId(), athlete.toText());

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
                // emit the input from mapper
                context.write(key, value);
            }
        }
    }

    // set the noc mapping
    public static void setNOCMapping (String fileName) {
        List<HashMap<String, String>> result = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(fileName))) {
            String[] parsedLine;
            // skip header
            reader.readNext();
            /*
            Build two Hashmaps, each one stores key:string -> value:string. The first one stores noc -> region values
            while the second one stores notes -> noc values. The idea here is that some countries have changed names
            or do not even exist, over all these years. Therefore, using the noc_regions.csv a mapping is done so each
            team in the olympics is replaced by its current region.
             */
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
        long start = System.currentTimeMillis();
        // set the noc mapping using the first file argument
        setNOCMapping(args[0]);

        // configurations for the Map Reduce Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "preprocessing");
        job.setJarByClass(Preprocessing.class);

        // Set Mapper, Combiner, Reducer classes
        job.setMapperClass(PreprocessingMapper.class);
        job.setCombinerClass(PreprocessingReducer.class);
        job.setReducerClass(PreprocessingReducer.class);

        // Set Output Key Value Classes
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // set the input and output files
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]),true);
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

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