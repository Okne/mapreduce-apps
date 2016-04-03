package com.epam.hadoop.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SecondarySortReducerTest {

    public ReduceDriver<CiWritable, IntWritable, CiWritable, IntWritable> reduceDriver;

    @Before
    public void setUp() throws Exception {
        SecondarySortReducer reducer = new SecondarySortReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void reduce_happyDay() throws Exception {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(2));
        values.add(new IntWritable(1));
        values.add(new IntWritable(3));
        values.add(new IntWritable(3));
        values.add(new IntWritable(0));

        reduceDriver.withInput(new CiWritable("id", "timestamp"), values);
        reduceDriver.withOutput(new CiWritable("id", "timestamp"), new IntWritable(2));

        reduceDriver.runTest();
    }

    @Test
    public void reduce_happyDayZeroImpressions() throws Exception {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(0));
        values.add(new IntWritable(2));
        values.add(new IntWritable(0));
        values.add(new IntWritable(3));
        values.add(new IntWritable(3));
        values.add(new IntWritable(0));

        reduceDriver.withInput(new CiWritable("id", "timestamp"), values);
        reduceDriver.withOutput(new CiWritable("id", "timestamp"), new IntWritable(0));

        reduceDriver.runTest();
    }
}
