package DataTransformer.DataTransformer.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import DataTransformer.DataTransformer.service.CopyFileToHDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

@Controller
public class FileUploadController {

    // Map to keep track of job statuses
    private final Map<String, String> jobStatusMap = new ConcurrentHashMap<>();

    @RequestMapping("/")
    public String index() {
        return "index"; // Ensure there's an index.html in src/main/resources/templates
    }

    @PostMapping("/uploadFile")
    public String uploadFile(@RequestParam("filePath") String filePath, Model model) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            model.addAttribute("message", "File does not exist or is not a valid file.");
            return "index"; // Redirect back to the index page with an error message
        }
        
        // Generate a unique job ID
        String jobId = UUID.randomUUID().toString();
        jobStatusMap.put(jobId, "IN_PROGRESS"); // Update job status to IN_PROGRESS

        // Run the file copy and job processing in a separate thread
        CompletableFuture.runAsync(() -> {
            try {
                CopyFileToHDFS.copyAndRunJob(file.getAbsolutePath());
                jobStatusMap.put(jobId, "COMPLETED"); // Update job status to COMPLETED
            } catch (Exception e) {
                jobStatusMap.put(jobId, "FAILED"); // Update job status to FAILED
                e.printStackTrace(); // Print the exception for debugging
            }
        }).exceptionally(e -> {
            // This handles any exceptions that were not caught above
            jobStatusMap.put(jobId, "FAILED");
            e.printStackTrace(); // Log the exception
            return null;
        });

        model.addAttribute("jobId", jobId);
        return "index"; // Redirect back to index page
    }

    // Method to check job status
    @GetMapping("/jobStatus/{jobId}")
    @ResponseBody
    public String checkJobStatus(@PathVariable String jobId) {
        return jobStatusMap.getOrDefault(jobId, "UNKNOWN");
    }
    
    @GetMapping("/listFiles")
    @ResponseBody
    public List<String> listFiles() throws IOException {
        List<String> fileNames = new ArrayList<>();
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000"); // Adjust the HDFS URI as needed
        configuration.setBoolean("fs.automatic.shutdown.hooks", false); // Sneha

        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path("/DataTransformer/Output_Files/");

        for (org.apache.hadoop.fs.FileStatus fileStatus : fs.listStatus(path)) {
            fileNames.add(fileStatus.getPath().getName());
        }

        return fileNames; // Return list of file names
    }
    
    @GetMapping("/downloadFile/{fileName}")
    @ResponseBody
    public ResponseEntity<String> downloadFile(@PathVariable String fileName) {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000"); // Adjust as needed
        FileSystem fs;

        try {
            fs = FileSystem.get(configuration);
            Path hdfsPath = new Path("/DataTransformer/Output_Files/" + fileName);
            Path localPath = new Path("/home/dell/Downloads/" + fileName);

            if (!fs.exists(hdfsPath)) {
                return ResponseEntity.status(404).body("File not found in HDFS.");
            }

            // Copy the file from HDFS to the local directory
            fs.copyToLocalFile(hdfsPath, localPath);

            return ResponseEntity.ok("File downloaded successfully");
        } catch (IOException e) {
            return ResponseEntity.status(500).body("Error downloading file: " + e.getMessage());
        }
    }
    
    @org.springframework.context.annotation.Configuration
    public class WebConfig implements WebMvcConfigurer {
        @Override
        public void addCorsMappings(CorsRegistry registry) {
            registry.addMapping("/**") // Allow all endpoints
                    .allowedOrigins("http://localhost:8080") // Replace with your frontend URL
                    .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                    .allowCredentials(true);
        }
    }
}
