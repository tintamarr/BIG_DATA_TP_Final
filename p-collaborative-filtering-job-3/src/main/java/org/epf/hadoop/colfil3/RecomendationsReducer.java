package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;

public class RecomendationsReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<SimpleEntry<String, Integer>> suggestions = new ArrayList<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            String otherUser = parts[0].trim();
            int commonRelations = Integer.parseInt(parts[1].trim());

            if (!otherUser.equals(key.toString())) {
                suggestions.add(new SimpleEntry<>(otherUser, commonRelations));
            }
        }

        suggestions.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        int count = 0;
        for (SimpleEntry<String, Integer> suggestion : suggestions) {
            if (count >= 5) break;
            context.write(key, new Text(suggestion.getKey() + "," + suggestion.getValue()));
            count++;
        }
    }
}
