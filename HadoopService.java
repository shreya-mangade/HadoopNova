package service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@Service
public class HadoopService {

    private static final String HDFS_URI = "hdfs://localhost:9000";  // Update with your HDFS URI
    private static final String HDFS_DIR = "/user/hadoop/uploads/";

    private FileSystem getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HDFS_URI);
        return FileSystem.get(URI.create(HDFS_URI), configuration);
    }

    public void uploadFileToHDFS(MultipartFile file) {
        try (FileSystem fs = getFileSystem()) {
            Path hdfsPath = new Path(HDFS_DIR + file.getOriginalFilename());
            fs.copyFromLocalFile(new Path(file.getOriginalFilename()), hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getFilesFromHDFS() {
        List<String> files = new ArrayList<>();
        try (FileSystem fs = getFileSystem()) {
//            for (Path file : fs.listStatus(new Path(HDFS_DIR))) {
//                files.add(file.getName());
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return files;
    }
}
