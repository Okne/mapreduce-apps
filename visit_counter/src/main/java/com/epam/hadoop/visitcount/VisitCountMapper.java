package com.epam.hadoop.visitcount;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VisitCountMapper extends Mapper<LongWritable, Text, Text, VisitsSpendsPair> {

    public static final int VALUE_COLUMNS_AMOUNT = 22;
    public static final int VALUE_USER_AGENT_COLUMN_NUMBER = 3;
    public static final int VALUE_IP_COLUMN_NUMBER = 4;
    public static final int VALUE_USER_BIDDING_PRICE_COLUMN_NUMBER = 18;

    public static final String COLUMN_VALUES_SEPARATOR = "\t";

    private final IntWritable one = new IntWritable(1);
    private final Text ip = new Text();
    private final VisitsSpendsPair pair = new VisitsSpendsPair();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.RECORD_COUNT).increment(1);

        String[] columnValues = value.toString().split(COLUMN_VALUES_SEPARATOR);
        if (columnValues != null && columnValues.length == VALUE_COLUMNS_AMOUNT) {
            try {
                ip.set(columnValues[VALUE_IP_COLUMN_NUMBER]);

                int biddingPrice = Integer.parseInt(columnValues[VALUE_USER_BIDDING_PRICE_COLUMN_NUMBER]);
                pair.set(one, new IntWritable(biddingPrice));

                context.write(ip, pair);

                incCounterForUserAgent(columnValues[VALUE_USER_AGENT_COLUMN_NUMBER], context);
            } catch (NumberFormatException e) {
                context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.PRICE_FORMAT_ERROR).increment(1);
            }
        } else {
            context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.LINE_FORMAT_ERRORS).increment(1);
        }

    }

    private void incCounterForUserAgent(String userAgentString, Context context) {
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
        Browser browser = userAgent.getBrowser().getGroup();
        switch (browser) {
            case FIREFOX:
                context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.FIREFOX_USERS).increment(1);
                break;
            case CHROME:
                context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.CHROME_USERS).increment(1);
                break;
            case SAFARI:
                context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.SAFARI_USERS).increment(1);
            case IE:
                context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.IE_USERS).increment(1);
                break;
            default:
                context.getCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.OTHER_BROWSER_USERS).increment(1);
                break;
        }
    }
}
