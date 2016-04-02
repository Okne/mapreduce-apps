package com.epam.hadoop.visitcount;

import com.google.common.base.Objects;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom {@link org.apache.hadoop.io.Writable} implementation
 * to store number of visits along with total spend amount per these visits
 */
public class VisitsSpendsPair implements WritableComparable<VisitsSpendsPair> {

    private IntWritable visits;
    private IntWritable spends;

    public VisitsSpendsPair() {
        set(new IntWritable(0), new IntWritable(0));
    }

    public VisitsSpendsPair(int visits, int spends) {
        set(new IntWritable(visits), new IntWritable(spends));
    }

    public VisitsSpendsPair(IntWritable visits, IntWritable spends) {
        set(visits, spends);
    }

    public void set(IntWritable visits, IntWritable spends) {
        this.visits = visits;
        this.spends = spends;
    }

    public IntWritable getVisits() {
        return visits;
    }

    public IntWritable getSpends() {
        return spends;
    }

    @Override
    public int compareTo(VisitsSpendsPair pair) {
        int res = visits.compareTo(pair.getVisits());
        if (res != 0) {
            return res;
        }
        return spends.compareTo(pair.getSpends());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        visits.write(out);
        spends.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        visits.readFields(in);
        spends.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VisitsSpendsPair that = (VisitsSpendsPair) o;
        return Objects.equal(visits, that.visits) &&
                Objects.equal(spends, that.spends);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(visits, spends);
    }

    @Override
    public String toString() {
        return visits + "\t" + spends;
    }
}
