package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    public void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        keyOut.set(value.getId1());
        valueOut.set(value.getId2());
        context.write(keyOut, valueOut);

        keyOut.set(value.getId2());
        valueOut.set(value.getId1());
        context.write(keyOut, valueOut);
    }
}
