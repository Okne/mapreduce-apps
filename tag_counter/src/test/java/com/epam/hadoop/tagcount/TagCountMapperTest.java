package com.epam.hadoop.tagcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TagCountMapperTest {

    public static final String CORRECT_INPUT_LINE = "53aa5317f3d33a8a3c157d314b5a72e8\t20130612233412741\tVh5_C5xbOqahjCk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; qdesk 2.4.1262.203; QQDownload 708)\t125.117.104.*\t94\t101\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_1\t300\t250\t0\t0\t100\t44966cc8da1ed40c95d59e863c8c75f0\t300\t3386\t282825712806\t0";
    public static final String EMPTY_TAGS_INPUT_LINE = "53aa5317f3d33a8a3c157d314b5a72e8\t20130612233412741\tVh5_C5xbOqahjCk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; qdesk 2.4.1262.203; QQDownload 708)\t125.117.104.*\t94\t101\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_1\t300\t250\t0\t0\t100\t44966cc8da1ed40c95d59e863c8c75f0\t300\t3386\t282162986020\t0";
    public static final String WRONG_CACHE_FORMAT_INPUT_LINE = "53aa5317f3d33a8a3c157d314b5a72e8\t20130612233412741\tVh5_C5xbOqahjCk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; qdesk 2.4.1262.203; QQDownload 708)\t125.117.104.*\t94\t101\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_1\t300\t250\t0\t0\t100\t44966cc8da1ed40c95d59e863c8c75f0\t300\t3386\t282825713006\t0";
    public static final String WRONG_FORMAT_INPUT_LINE = "53aa5317f3d33a8a3c157d314b5a72e8\t20130612233412741\tVh5_C5xbOqahjCk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; qdesk 2.4.1262.203; QQDownload 708)\t125.117.104.*\t94\t101\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_1\t300\t250\t0\t0\t100\t44966cc8da1ed40c95d59e863c8c75f0\t300\t3386\t282825712806";

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() throws Exception {
        TagCountMapper mapper = new TagCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void map_happyDay() throws Exception {
        mapDriver.addCacheFile("user.profile.tags.us");

        mapDriver.withInput(new LongWritable(), new Text(CORRECT_INPUT_LINE));
        mapDriver.withOutput(new Text("usd"), new IntWritable(1));
        mapDriver.withOutput(new Text("and"), new IntWritable(1));
        mapDriver.withOutput(new Text("to"), new IntWritable(1));
        mapDriver.withOutput(new Text("the"), new IntWritable(1));
        mapDriver.withOutput(new Text("new"), new IntWritable(1));
        mapDriver.withOutput(new Text("for"), new IntWritable(1));
        mapDriver.withOutput(new Text("republic"), new IntWritable(1));
        mapDriver.withOutput(new Text("in"), new IntWritable(1));
        mapDriver.withOutput(new Text("you"), new IntWritable(1));
        mapDriver.withOutput(new Text("dom"), new IntWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void map_wrongLineFormat() throws Exception {
        mapDriver.addCacheFile("user.profile.tags.us");
        mapDriver.withInput(new LongWritable(), new Text(WRONG_FORMAT_INPUT_LINE));

        mapDriver.runTest();

        assertEquals("Expect 1 counter increment", 1, mapDriver.getCounters().findCounter(TagCountMapper.TAG_COUNT_APP_COUNTERS.INPUT_VALUE_FORMAT_ERROR).getValue());
    }

    @Test
    public void map_emptyTagsList() throws Exception {
        mapDriver.addCacheFile("user.profile.tags.us");
        mapDriver.withInput(new LongWritable(), new Text(EMPTY_TAGS_INPUT_LINE));

        mapDriver.runTest();

        assertEquals("Expect 1 counter increment", 1, mapDriver.getCounters().findCounter(TagCountMapper.TAG_COUNT_APP_COUNTERS.EMPTY_TAGS).getValue());
    }

    @Test
    public void map_wrongCachedInputLineFormat() throws Exception {
        mapDriver.addCacheFile("user.profile.tags.us");
        mapDriver.withInput(new LongWritable(), new Text(WRONG_CACHE_FORMAT_INPUT_LINE));

        mapDriver.runTest();

        assertEquals("Expect 1 counter increment", 1, mapDriver.getCounters().findCounter(TagCountMapper.TAG_COUNT_APP_COUNTERS.CACHE_FILE_LINE_FORMAT_ERROR).getValue());
    }

    @Test
    public void map_cacheFileMissing() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text(CORRECT_INPUT_LINE));

        mapDriver.runTest();

        assertEquals("Expect 1 counter increment", 1, mapDriver.getCounters().findCounter(TagCountMapper.TAG_COUNT_APP_COUNTERS.EMPTY_CACHED_FILES).getValue());
    }
}
