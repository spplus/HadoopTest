package com.hadoop.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
* User: wyp
* Date: 13-10-25
* Time: 下午3:36
* Email:wyphao.2007@163.com
*/
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, 
                    Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, 
            Context output) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        
        for (IntWritable val : values) {
        	maxValue = Math.max(maxValue, val.get());
        }
        
        output.write(key, new IntWritable(maxValue));
    }
}
