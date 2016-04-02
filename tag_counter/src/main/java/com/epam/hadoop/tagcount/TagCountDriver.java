package com.epam.hadoop.tagcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TagCountDriver extends Configured implements Tool {

    public static final double FAILED_RATIO_THRESHOLD = 0.05;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 0 && args.length != 2) {
            System.err.println("Usage: hdfsInputFileOrDirectory hdfsOutputDirectory"); // hdfsTagsFile");
            System.exit(2);
        }

        String hdfsInputFileOrDirectory = args[0];
        String hdfsOutputDirectory = args[1];

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(hdfsInputFileOrDirectory));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputDirectory));

        job.setMapperClass(TagCountMapper.class);
        job.setCombinerClass(TagCountReducer.class);
        job.setReducerClass(TagCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int intermediateResult = job.waitForCompletion(true) ? 0 : 1;
        int finalResult = intermediateResult;

        //check failed lines ratio
        if (intermediateResult == 0) {
            //is job finish successfully
            long recordCount = job.getCounters()
                    .findCounter(TagCountMapper.TAG_COUNT_APP_COUNTERS.RECORD_COUNT).getValue();
            long wrongFormatRecordCount = job.getCounters()
                    .findCounter(TagCountMapper.TAG_COUNT_APP_COUNTERS.INPUT_VALUE_FORMAT_ERROR).getValue();
            if (recordCount != 0) {
                double failedRatio = wrongFormatRecordCount * 100 / recordCount;
                if (failedRatio > FAILED_RATIO_THRESHOLD) {
                    System.err.println("Failed records ratio threshold exceded. Job failed.");
                    finalResult = 1;
                } else {
                    System.out.println("Job finished successfully. Failed ration: " + failedRatio + "; threshold: " + FAILED_RATIO_THRESHOLD);
                }
            }
        }

        return finalResult;

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TagCountDriver(), args);
        System.exit(res);
    }
}
