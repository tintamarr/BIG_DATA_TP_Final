package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonRelationshipsReducer extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {
    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        boolean directlyConnected = false;

        for (IntWritable value : values) {
            if (value.get() == 0) {
                directlyConnected = true;
                break;
            }
            sum += value.get();
        }

        if (!directlyConnected && sum > 0) {
            context.write(key, new IntWritable(sum));
        }
    }
}
