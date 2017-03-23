package com.hadoop.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
* User: wyp
* Date: 13-10-25
* Time: 下午3:26
* Email:wyphao.2007@163.com
*/
public class MaxTemperatureMapper extends  Mapper<Object, Text, Text, IntWritable>{
    private static final int MISSING = 9999;

    @Override
    public void map(Object key, Text value, Context output) throws IOException, InterruptedException  {

        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature;
        if(line.charAt(87) == '+'){
            airTemperature = Integer.parseInt(line.substring(88, 92));
        }else{
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }

        String quality = line.substring(92, 93);
        if(airTemperature != MISSING && quality.matches("[01459]")){
            output.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
