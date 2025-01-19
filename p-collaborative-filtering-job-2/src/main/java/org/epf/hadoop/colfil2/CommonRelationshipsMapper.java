package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class CommonRelationshipsMapper extends Mapper<Object, Text, UserPair, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final IntWritable zero = new IntWritable(0);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] userAndRelations = value.toString().split("\t");
        if (userAndRelations.length != 2) return;

        String user = userAndRelations[0];
        String[] relations = userAndRelations[1].split(",");

        for (int i = 0; i < relations.length; i++) {
            for (int j = i + 1; j < relations.length; j++) {
                UserPair pair = new UserPair(relations[i], relations[j]);
                context.write(pair, one);
            }
        }

        for (String relation : relations) {
            UserPair directPair = new UserPair(user, relation);
            context.write(directPair, zero);
        }
    }
}
