package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder friends = new StringBuilder();
        for (Text val : values) {
            if (friends.length() > 0) {
                friends.append(",");
            }
            friends.append(val.toString());
        }
        result.set(friends.toString());
        context.write(key, result);
    }
}
