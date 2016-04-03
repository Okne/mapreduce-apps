package com.epam.hadoop.secondsort;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SecondarySortReducer extends Reducer<CiWritable, IntWritable, CiWritable, IntWritable> {

    private int maxImpressionCount  = 0;
    private String maxImprCountId;
    private IntWritable res = new IntWritable();

    @Override
    protected void reduce(CiWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int imprCount = 0;

        Iterator<IntWritable> it = values.iterator();
        while(it.hasNext()) {
            int streamId = it.next().get();
            if (streamId == SecondarySortMapper.Stream.IMPRESSION.ordinal()) {
                imprCount++;
            }
        }
        res.set(imprCount);
        context.write(key, res);

        if (imprCount > maxImpressionCount) {
            maxImprCountId = key.getId().toString();
            maxImpressionCount = imprCount;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("MAX: IPinYouId - " + maxImprCountId + " count - " + maxImpressionCount);
    }
}
