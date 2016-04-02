package com.epam.hadoop.visitcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VisitCountMapperTest {

    public static final String CORRECT_INPUT_LINE_IE = "53aa5317f3d33a8a3c157d314b5a72e8\t20130612233412741\tVh5_C5xbOqahjCk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; qdesk 2.4.1262.203; QQDownload 708)\t125.117.104.*\t94\t101\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_1\t300\t250\t0\t0\t100\t44966cc8da1ed40c95d59e863c8c75f0\t300\t3386\t282825712806\t0";
    public static final String CORRECT_INPUT_LINE_CHROME = "3af9981cbde9349a8b1e5dd223938274\t20130606184922652\tnull\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t14.110.71.*\t275\t275\t3\tDDTSQuf0MTTNaqKIvMpENpn\tae25820e75d4e99d0ea3ef4c5e9b108d\tnull\tAstro_2nd_Width1\t960\t90\t0\t0\t50\tfb5afa9dba1274beaf3dad86baf97e89\t300\t1458\t282825712851\t0";
    public static final String CORRECT_INPUT_LINE_FIREFOX = "a77b1fc78d0ff5d4b7b1881654008def\t20130606184923207\tnull\tMozilla/5.0 (Macintosh; U; Intel Mac OS X 10.7; zh-CN; rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13\t117.140.158.*\t1\t1\t3\tDFKXB19rg5scFsf\te42940ef37d320d11b0c06c7944ae63e\tnull\tBaby_Width2\t960\t90\t0\t0\t20\tfb5afa9dba1274beaf3dad86baf97e89\t300\t1458\t282825712806\t0";
    public static final String INCORRECT_BIDDING_PRICE_FORMAT_INPUT_LINE = "53aa5317f3d33a8a3c157d314b5a72e8\t20130612233412741\tVh5_C5xbOqahjCk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; qdesk 2.4.1262.203; QQDownload 708)\t125.117.104.*\t94\t101\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_1\t300\t250\t0\t0\t100\t44966cc8da1ed40c95d59e863c8c75f0\t300usd\t3386\t282825712806\t0";
    public static final String FORMAT_ERROR_INPUT_LINE = "53aa5317f3d33a8a3c157d314b5a72e8\tVh5_C5xbOqahjCk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; qdesk 2.4.1262.203; QQDownload 708)\t125.117.104.*\t94\t101\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_1\t300\t250\t0\t0\t100\t44966cc8da1ed40c95d59e863c8c75f0\t300\t3386\t282825712806\t0";

    MapDriver<LongWritable, Text, Text, VisitsSpendsPair> mapDriver;

    @Before
    public void setup() {
        VisitCountMapper mapper = new VisitCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void map_happyDay_IE() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text(CORRECT_INPUT_LINE_IE));
        mapDriver.withOutput(new Text("125.117.104.*"), new VisitsSpendsPair(new IntWritable(1), new IntWritable(300)));

        mapDriver.runTest();
        assertEquals("Expect 1 counter increment", 1,
                mapDriver.getCounters().findCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.IE_USERS).getValue());

    }

    @Test
    public void map_happyDay_Chrome() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text(CORRECT_INPUT_LINE_CHROME));
        mapDriver.withOutput(new Text("14.110.71.*"), new VisitsSpendsPair(new IntWritable(1), new IntWritable(300)));

        mapDriver.runTest();
        assertEquals("Expect 1 counter increment", 1,
                mapDriver.getCounters().findCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.CHROME_USERS).getValue());

    }

    @Test
    public void map_happyDay_Firefox() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text(CORRECT_INPUT_LINE_FIREFOX));
        mapDriver.withOutput(new Text("117.140.158.*"), new VisitsSpendsPair(new IntWritable(1), new IntWritable(300)));

        mapDriver.runTest();
        assertEquals("Expect 1 counter increment", 1,
                mapDriver.getCounters().findCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.FIREFOX_USERS).getValue());

    }

    @Test
    public void map_lineFormatError() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text(FORMAT_ERROR_INPUT_LINE));

        mapDriver.runTest();
        assertEquals("Expect 1 counter increment", 1,
                mapDriver.getCounters().findCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.LINE_FORMAT_ERRORS).getValue());
    }

    @Test
    public void map_biddingPriceFormatError() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text(INCORRECT_BIDDING_PRICE_FORMAT_INPUT_LINE));

        mapDriver.runTest();
        assertEquals("Expect 1 counter increment", 1,
                mapDriver.getCounters().findCounter(VisitCountDriver.VISIT_COUNT_JOB_COUNTER.PRICE_FORMAT_ERROR).getValue());
    }
}
