package org.epf.hadoop.colfil1;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        boolean hasNext = lineRecordReader.nextKeyValue();
        if (hasNext) {
            currentKey.set(lineRecordReader.getCurrentKey().get());

            String line = lineRecordReader.getCurrentValue().toString();
            String[] sections = line.split(",");
            if (sections.length >= 1) {
                String friendshipPart = sections[0];
                String[] ids = friendshipPart.split("<->");
                if (ids.length == 2) {
                    currentValue.setId1(ids[0]);
                    currentValue.setId2(ids[1]);
                }
            }
        }
        return hasNext;
    }


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Relationship getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
