package service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.stereotype.Component;

@Component
public class CSVToJSONJob {

	public static void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); // Adjust the HDFS URI as needed
        
        Job job = Job.getInstance(conf, "CSV to JSON Converter");
        job.setJarByClass(CSVToJSONJob.class);
        
        // Set Mapper and Reducer classes
        job.setMapperClass(CSVToJsonMapper.class);
        job.setReducerClass(CSVToJsonReducer.class);

     // Set output types
        job.setOutputKeyClass(NullWritable.class);
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

        // Set input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
