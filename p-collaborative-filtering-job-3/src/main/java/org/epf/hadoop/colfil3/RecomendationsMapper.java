package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.naming.Context;
import java.io.IOException;

public class RecomendationsMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split("\t");
        String userPair = parts[0];
        int commonRelations = Integer.parseInt(parts[1]);

        String[] users = userPair.split("\\.");
        String userA = users[0];
        String userB = users[1];

        context.write(new Text(userA), new Text(userB + "," + commonRelations));

        context.write(new Text(userB), new Text(userA + "," + commonRelations));
    }
}
