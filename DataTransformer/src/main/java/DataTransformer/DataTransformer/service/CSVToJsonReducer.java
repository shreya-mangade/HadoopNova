package DataTransformer.DataTransformer.service;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CSVToJsonReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // Output each JSON line. You can aggregate if necessary.
            context.write(null, value);
        }
    }
}