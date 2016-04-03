package com.epam.hadoop.secondsort;

import com.google.common.base.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CiWritable implements WritableComparable<CiWritable> {

    private Text id;
    private Text timestamp;

    public CiWritable() {
        set(new Text(), new Text());
    }

    public CiWritable(Text id, Text timestamp) {
        set(id, timestamp);
    }

    public CiWritable(String id, String timestamp) {
        set(new Text(id), new Text(timestamp));
    }

    public void set(Text id, Text timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CiWritable o) {
        int comIdRes = getId().compareTo(o.getId());
        if (comIdRes != 0) {
            return  comIdRes;
        }
        return getTimestamp().compareTo(o.getTimestamp());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        timestamp.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CiWritable that = (CiWritable) o;
        return Objects.equal(id, that.id) &&
                Objects.equal(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, timestamp);
    }

    @Override
    public String toString() {
        return id + "\t" + timestamp;
    }

    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = id;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Text timestamp) {
        this.timestamp = timestamp;
    }
}
