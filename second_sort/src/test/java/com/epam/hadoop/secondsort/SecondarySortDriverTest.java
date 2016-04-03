package com.epam.hadoop.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;

public class SecondarySortDriverTest {
    MapReduceDriver<LongWritable, Text, CiWritable, IntWritable, CiWritable, IntWritable> mapReduceDriver;

    @Before
    public void setup() {
        SecondarySortMapper mapper = new SecondarySortMapper();
        SecondarySortReducer reducer = new SecondarySortReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
}
