package com.adbibo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

    private Text yearMonth = new Text();

    public Text getYearMonth() {
        return yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    private Text day = new Text();
    private IntWritable temperature = new IntWritable();


    public int compareTo(DateTemperaturePair pair) {

        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());


        return 0;
    }
}
