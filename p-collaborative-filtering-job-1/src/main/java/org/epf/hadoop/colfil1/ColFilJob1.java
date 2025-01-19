package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ColFilJob1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ColFilJob1(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RelationshipJob <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Relationship Job");
        job.setJarByClass(ColFilJob1.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(RelationshipInputFormat.class);


        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
