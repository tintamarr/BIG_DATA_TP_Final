package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Set<String> friendships = new HashSet<>();

        for (Text value : values) {
            friendships.add(value.toString());
        }


        StringBuilder friends = new StringBuilder();
        for (String friendship : friendships) {
            if (friends.length() > 0) {
                friends.append(", ");
            }
            friends.append(friendship);
        }

        context.write(key, new Text(friends.toString()));
    }

}