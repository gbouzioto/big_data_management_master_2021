// Package imports
package project.preprocessing.main;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringReader;

import static project.utils.Constants.ONE;


public class ExploratoryAnalyser {

        public static class ExploratoryAnalyserMapper
                extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

            public void map(LongWritable key, Text value, Context context
            ) throws IOException, InterruptedException {
                // skip header
                if (key.get() == 0) {
                    return;
                }
                String line = value.toString();
                // parse each line
                try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
                    String[] parsedLine = csvReader.readNext();
                    // parse each column
                    for (int i = 0; i < parsedLine.length; i++) {
                        if (parsedLine[i].equals("NA")) {
                            // emit the index of the column that has null value and 1 as value
                            context.write(new IntWritable(i), ONE);
                        }
                    }

                } catch (CsvValidationException e) {
                    e.printStackTrace();
                }
            }
        }

        public static class ExploratoryAnalyserReducer
                extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

            public void reduce(IntWritable key, Iterable<IntWritable> values,
                               Context context
            ) throws IOException, InterruptedException {
                // calculate the sum of the missing values per index
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                // emit the index and the missing values count
                context.write(key, new IntWritable(sum));
                }
            }




        public static void main(String[] args) throws Exception {
            long start = System.currentTimeMillis();
            // configurations for the Map Reduce Job
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "exploratory analysis");
            job.setJarByClass(project.preprocessing.main.Preprocessing.class);

            // Set Mapper, Combiner, Reducer classes
            job.setMapperClass(ExploratoryAnalyserMapper.class);
            job.setCombinerClass(ExploratoryAnalyserReducer.class);
            job.setReducerClass(ExploratoryAnalyserReducer.class);

            // Set Output Key Value Classes
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            // set the input and output files
            FileSystem fs = FileSystem.get(conf);

            if(fs.exists(new Path(args[1]))) {
                fs.delete(new Path(args[1]),true);
            }

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

