package com.epam.hadoop.visitcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VisitCountReducerTest {

    public ReduceDriver<Text, VisitsSpendsPair, Text, VisitsSpendsPair> reduceDriver;

    @Before
    public void setUp() throws Exception {
        VisitCountReducer reducer = new VisitCountReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void reduce_happyDay() throws Exception {
        List<VisitsSpendsPair> values = new ArrayList<>();
        values.add(new VisitsSpendsPair(1, 500));
        values.add(new VisitsSpendsPair(1, 300));
        values.add(new VisitsSpendsPair(1, 200));

        reduceDriver.withInput(new Text("ip"), values);
        reduceDriver.withOutput(new Text("ip"), new VisitsSpendsPair(3, 1000));

        reduceDriver.runTest();
    }
}
