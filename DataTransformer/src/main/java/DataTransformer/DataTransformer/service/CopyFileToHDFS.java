package DataTransformer.DataTransformer.service;

import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;

public class CopyFileToHDFS {

    public static void copyAndRunJob(String localFilePath) {
        String hdfsDestinationPath = "/DataTransformer/Input_Files/" + new File(localFilePath).getName();
        String mapRedDestinationPath = "/DataTransformer/Output_Files/" + new File(localFilePath).getName().replace(".csv", ".json");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); // Adjust the HDFS URI as needed

        try {
            FileSystem fs = FileSystem.get(conf);
            Path hdfsPath = new Path(hdfsDestinationPath);
            
            // Check if file already exists in HDFS
            if (!fs.exists(hdfsPath)) {
                fs.copyFromLocalFile(new Path(localFilePath), hdfsPath);
                System.out.println("File copied to HDFS at: " + hdfsDestinationPath);

                // Start MapReduce job after successful copy
                CSVToJSONJob.runJob(hdfsDestinationPath, mapRedDestinationPath);
            } else {
                System.out.println("File already exists in HDFS at " + hdfsPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
