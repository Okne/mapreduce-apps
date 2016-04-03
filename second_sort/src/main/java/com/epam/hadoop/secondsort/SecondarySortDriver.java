package com.epam.hadoop.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {

    public static enum COUNTER {
        RECORD_COUNT, LINE_FORMAT_ERRORS, WRONG_STREAM_ID
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 0 && args.length != 2) {
            System.err.println("Usage: hdfsInputFileOrDirectory hdfsOutputDirectory");
            System.exit(2);
        }

        String hdfsInputFileOrDirectory = args[0];
        String hdfsOutputDirectory = args[1];

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(hdfsInputFileOrDirectory));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputDirectory));

        job.setNumReduceTasks(1);

        job.setMapperClass(SecondarySortMapper.class);
        job.setPartitionerClass(CiPartitioner.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setSortComparatorClass(CiKeyComparator.class);
        job.setGroupingComparatorClass(CiKeyGroupingComparator.class);

        job.setOutputKeyClass(CiWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SecondarySortDriver(), args);
        System.exit(res);
    }

    public static class CiPartitioner extends Partitioner<CiWritable, IntWritable> {
        @Override
        public int getPartition(CiWritable key, IntWritable value, int numPartitions) {
            return key.getId().hashCode() % numPartitions;
        }
    }

    public static class CiKeyComparator extends WritableComparator {
        public CiKeyComparator() {
            super(CiWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CiWritable ci1 = (CiWritable) a;
            CiWritable ci2 = (CiWritable) b;
            int idCmpRes = ci1.getId().compareTo(ci2.getId());
            if (idCmpRes != 0) {
                return idCmpRes;
            }
            return ci1.getTimestamp().compareTo(ci2.getTimestamp());
        }
    }

    public static class CiKeyGroupingComparator extends WritableComparator {
        public CiKeyGroupingComparator() {
            super(CiWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CiWritable ci1 = (CiWritable) a;
            CiWritable ci2 = (CiWritable) b;
            return ci1.getId().compareTo(ci2.getId());
        }
    }
}
