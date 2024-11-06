package DataTransformer.DataTransformer.service;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CSVToJsonMapper extends Mapper<Object, Text, Text, Text> {
	private String[] headers;
    private boolean isHeaderRow = true;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	// Split CSV row by commas
        String[] columns = value.toString().split(",");

        // If it’s the first row, treat it as the header and skip it in the output
        if (isHeaderRow) {
            headers = columns;
            isHeaderRow = false;
            return;
        }

        // Map column names to values dynamically
        Map<String, String> jsonMap = new HashMap<>();
        for (int i = 0; i < columns.length && i < headers.length; i++) {
            jsonMap.put(headers[i], columns[i]);
        }

        // Convert the map to a JSON string
        String jsonString = jsonMapper.writeValueAsString(jsonMap);

        // Output the JSON with a dummy key or unique identifier
        context.write(new Text("jsonOutput"), new Text(jsonString));
    }
}

