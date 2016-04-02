package com.epam.hadoop.visitcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class VisitCountReducer extends Reducer<Text, VisitsSpendsPair, Text, VisitsSpendsPair> {

    private final VisitsSpendsPair res = new VisitsSpendsPair();

    @Override
    protected void reduce(Text key, Iterable<VisitsSpendsPair> values, Context context) throws IOException, InterruptedException {
        int visitCount = 0;
        int sumSpend = 0;

        Iterator<VisitsSpendsPair> it = values.iterator();
        while(it.hasNext()) {
            VisitsSpendsPair p = it.next();
            visitCount += p.getVisits().get();
            sumSpend += p.getSpends().get();
        }
        res.set(new IntWritable(visitCount), new IntWritable(sumSpend));
        context.write(key, res);
    }
}
