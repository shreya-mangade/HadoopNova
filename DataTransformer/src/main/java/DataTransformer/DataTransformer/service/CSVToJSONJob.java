package DataTransformer.DataTransformer.service;

import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import DataTransformer.DataTransformer.DataTransformerApplication;

@Component
public class CSVToJSONJob {
    @Async
	public static void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); // Adjust the HDFS URI as needed
        conf.set("mapreduce.framework.name", "yarn");
        
        Job job = Job.getInstance(conf, "CSV to JSON Converter");
        //job.setJarByClass(DataTransformerApplication.class);
        job.setJar("/home/dell/Downloads/DataTransformer/target/DataTransformer-0.0.1-SNAPSHOT-hadoop.jar");
        job.setMapperClass(CSVToJsonMapper.class);
        job.setReducerClass(CSVToJsonReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        Path outputDir = new Path(outputPath);
        
        // Ensure output directory doesn't exist
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
