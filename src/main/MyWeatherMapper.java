package main;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyWeatherMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public static final int MISSING = 9999; 
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		if(!(line.length() == 0)) {
			
			String date = line.substring(6, 14);
			
			double max_temp = Double.parseDouble(line.substring(39, 45).trim());
			double min_temp = Double.parseDouble(line.substring(47, 53).trim());
			
			if(max_temp > 28.0) {
				context.write(new Text("It\'s a Hot Day : " + date), new Text(String.valueOf(max_temp)));
			}
			
			if(min_temp < 10.0) {
				context.write(new Text("It\'s a Cold Day : " + date), new Text(String.valueOf(min_temp)));
			}
		}
		
	}
	
}
