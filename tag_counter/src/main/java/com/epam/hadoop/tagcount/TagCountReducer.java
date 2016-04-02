package com.epam.hadoop.tagcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TagCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable tagsCount = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
            count += it.next().get();
        }

        tagsCount.set(count);
        context.write(key, tagsCount);
    }
}
