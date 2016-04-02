package com.epam.hadoop.tagcount;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class TagCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final String CACHE_FILE_NAME = "user.profile.tags.us";
    public static final int CACHE_COLUMNS_AMOUNT = 6;
    public static final int CACHE_TAGS_COLUMN_NUMBER = 1;

    public static final int CACHE_USER_TAGS_ID_COLUMN_NUMBER = 0;
    public static final int VALUE_COLUMNS_AMOUNT = 22;
    public static final int VALUE_USER_TAG_ID_COLUMN_NUMBER = 20;

    public static final String TAGS_SEPARATOR = ",";
    public static final String COLUMN_VALUES_SEPARATOR = "\t";

    public enum TAG_COUNT_APP_COUNTERS {
        RECORD_COUNT, EMPTY_TAGS, INPUT_VALUE_FORMAT_ERROR, CACHE_FILE_EXISTS, GENERIC_ERROR, CACHE_FILE_LINE_FORMAT_ERROR, EMPTY_CACHED_FILES
    }

    private Text tag = new Text();
    private final IntWritable one = new IntWritable(1);
    private Map<String, String[]> userTagsMap = new HashMap<>();
    private String[] emptyTagsArray = ArrayUtils.EMPTY_STRING_ARRAY;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] localCacheFiles  = context.getLocalCacheFiles();

        if (localCacheFiles != null) {
            for (Path filePath : localCacheFiles) {
                if (filePath.getName().toLowerCase().equals(CACHE_FILE_NAME)) {
                    context.getCounter(TAG_COUNT_APP_COUNTERS.CACHE_FILE_EXISTS).increment(1);
                    loadUserTagsIntoMap(filePath, context);
                }
            }
        } else {
            context.getCounter(TAG_COUNT_APP_COUNTERS.EMPTY_CACHED_FILES).increment(1);
        }
    }

    protected void loadUserTagsIntoMap(Path filePath, Context context) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath.toString()));
            String line = null;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                //skipp line with headers
                if (lineNum == 0) {
                    lineNum++;
                    continue;
                }

                String[] columnValues = line.split(COLUMN_VALUES_SEPARATOR);
                if (columnValues.length == CACHE_COLUMNS_AMOUNT) {
                    String tags = columnValues[CACHE_TAGS_COLUMN_NUMBER];
                    String[] tagsArray = StringUtils.isNotBlank(tags) ? tags.split(TAGS_SEPARATOR) : emptyTagsArray;
                    userTagsMap.put(columnValues[CACHE_USER_TAGS_ID_COLUMN_NUMBER], tagsArray);
                } else {
                    context.getCounter(TAG_COUNT_APP_COUNTERS.CACHE_FILE_LINE_FORMAT_ERROR).increment(1);
                }

                lineNum++;
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File with user tags is not found in distributed cache. Use -file %file_name% as param", e);
        } catch (IOException e) {
            context.getCounter(TAG_COUNT_APP_COUNTERS.GENERIC_ERROR).increment(1);
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(TAG_COUNT_APP_COUNTERS.RECORD_COUNT).increment(1);

        String[] columnValues = value.toString().split(COLUMN_VALUES_SEPARATOR);
        if (columnValues.length == VALUE_COLUMNS_AMOUNT) {
            String userTagsId = columnValues[VALUE_USER_TAG_ID_COLUMN_NUMBER];
            String[] tagsArray = userTagsMap.get(userTagsId);
            if (tagsArray != null && tagsArray.length != 0) {
                for (String tagValue: tagsArray) {
                    tag.set(tagValue);
                    context.write(tag, one);
                }
            } else {
                context.getCounter(TAG_COUNT_APP_COUNTERS.EMPTY_TAGS).increment(1);
            }
        } else {
            context.getCounter(TAG_COUNT_APP_COUNTERS.INPUT_VALUE_FORMAT_ERROR).increment(1);
        }
    }
}
