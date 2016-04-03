package com.epam.hadoop.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CiWritable, IntWritable> {

    public enum Stream {
        UNKNOWN, IMPRESSION, CLICK, CONVERSION;

        public static Stream parse(String id) throws IllegalArgumentException {
            Stream stream = null;
            try {
                int parsedId = Integer.parseInt(id);
                switch (parsedId) {
                    case 1:
                        stream = IMPRESSION;
                        break;
                    case 2:
                        stream = CLICK;
                        break;
                    case 3:
                        stream = CONVERSION;
                        break;
                    default:
                        stream =UNKNOWN;
                        break;
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(e);
            }
            return stream;
        }
    }

    public static final int VALUE_COLUMNS_AMOUNT = 22;
    public static final int VALUE_TIMESTAMP_COLUMN_NUMBER = 1;
    public static final int VALUE_IPINYOU_ID_COLUMN_NUMBER = 2;
    public static final int VALUE_STREAM_ID_COLUMN_NUMBER = 21;

    public static final String COLUMN_VALUES_SEPARATOR = "\t";

    private final Text timestamp = new Text();
    private final Text iPinYouId = new Text();
    private final IntWritable streamId = new IntWritable();
    private final CiWritable compId = new CiWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(SecondarySortDriver.COUNTER.RECORD_COUNT).increment(1);

        String[] columnValues = value.toString().split(COLUMN_VALUES_SEPARATOR);
        if (columnValues != null && columnValues.length == VALUE_COLUMNS_AMOUNT) {
            timestamp.set(columnValues[VALUE_IPINYOU_ID_COLUMN_NUMBER]);
            iPinYouId.set(columnValues[VALUE_TIMESTAMP_COLUMN_NUMBER]);
            compId.set(timestamp, iPinYouId);

            Stream stream = null;
            try {
                stream = Stream.parse(columnValues[VALUE_STREAM_ID_COLUMN_NUMBER]);
            } catch (IllegalArgumentException e) {
                stream = Stream.UNKNOWN;
                context.getCounter(SecondarySortDriver.COUNTER.WRONG_STREAM_ID).increment(1);
            }
            streamId.set(stream.ordinal());

            context.write(compId, streamId);
        } else {
            context.getCounter(SecondarySortDriver.COUNTER.LINE_FORMAT_ERRORS).increment(1);
        }
    }
}
