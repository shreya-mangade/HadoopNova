package service;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVToJsonMapper extends Mapper<Object, Text, NullWritable, Text> {

    private String[] headers = null;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the CSV line by comma
        String[] columns = value.toString().split(",");

        // Check if headers are initialized
        if (headers == null) {
            // First row: initialize headers as keys
            headers = columns;
        } else {
            // Convert each row to JSON using headers as keys
            StringBuilder jsonBuilder = new StringBuilder("{");
            for (int i = 0; i < headers.length; i++) {
                jsonBuilder.append("\"").append(headers[i].trim()).append("\": ");
                jsonBuilder.append("\"").append(i < columns.length ? columns[i].trim() : "").append("\"");
                
                // Add comma except for the last key-value pair
                if (i < headers.length - 1) {
                    jsonBuilder.append(", ");
                }
            }
            jsonBuilder.append("}");

            // Write the JSON output
            context.write(NullWritable.get(), new Text(jsonBuilder.toString()));
        }
    }
}
