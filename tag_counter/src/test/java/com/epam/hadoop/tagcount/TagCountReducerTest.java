package com.epam.hadoop.tagcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TagCountReducerTest {

    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() throws Exception {
        TagCountReducer reducer = new TagCountReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void reduce_happyDay() throws Exception {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(2));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new Text("tag"), values);
        reduceDriver.withOutput(new Text("tag"), new IntWritable(4));

        reduceDriver.runTest();
    }
}
