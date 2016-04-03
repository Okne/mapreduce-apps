package com.epam.hadoop.visitcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VisitCountDriver extends Configured implements Tool {

    public enum VISIT_COUNT_JOB_COUNTER {
        RECORD_COUNT, LINE_FORMAT_ERRORS, PRICE_FORMAT_ERROR, FIREFOX_USERS, IE_USERS, SAFARI_USERS, CHROME_USERS, OTHER_BROWSER_USERS
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
        SequenceFileOutputFormat.setOutputPath(job , new Path(hdfsOutputDirectory));

        job.setMapperClass(VisitCountMapper.class);
        job.setCombinerClass(VisitCountReducer.class);
        job.setReducerClass(VisitCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VisitsSpendsPair.class);

        int res = job.waitForCompletion(true) ? 0 : 1;

        System.out.println("IE_USERS: " + job.getCounters().findCounter(VISIT_COUNT_JOB_COUNTER.IE_USERS).getValue());
        System.out.println("CHROME_USERS: " + job.getCounters().findCounter(VISIT_COUNT_JOB_COUNTER.CHROME_USERS).getValue());
        System.out.println("FIREFOX_USERS: " + job.getCounters().findCounter(VISIT_COUNT_JOB_COUNTER.FIREFOX_USERS).getValue());
        System.out.println("SAFARI_USERS: " + job.getCounters().findCounter(VISIT_COUNT_JOB_COUNTER.SAFARI_USERS).getValue());
        System.out.println("OTHER_BROWSER_USERS: " + job.getCounters().findCounter(VISIT_COUNT_JOB_COUNTER.OTHER_BROWSER_USERS).getValue());

        return res;
    }

    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VisitCountDriver(), args);
        System.exit(res);
    }
}
