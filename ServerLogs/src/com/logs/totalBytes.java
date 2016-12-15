
package com.logs;
import java.io.IOException;

import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class totalBytes {

	public static class byteMapper extends Mapper<Object, Text, Text, IntWritable> {
		// Our output key and value Writables
		public static int count=0;
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			count++;
			// Parse the input string into a nice map
			try
			{
			StringTokenizer stringTokenizer=new StringTokenizer(value.toString()," ");
        	String bytes="";
        	while(stringTokenizer.hasMoreTokens())
        	  {
        		bytes=stringTokenizer.nextToken();
        	  }
        	Integer memory=Integer.parseInt(bytes);
        	String k="Total bytes by "+count+" Visitor";
        	context.write(new Text(k), new IntWritable(memory));	
			}
			catch (Exception e) 
			{
				// TODO: handle exception
			}
		}

		
	}

	public static class byteReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public static int sum=0;
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			 	
	            for (IntWritable val : values)
	            {
	                sum += val.get();
	            }
		}
		 @Override
	      protected void cleanup(Context context) throws IOException, InterruptedException 
		 { 
			 context.write(new Text("The total no of bytes transferred:"), new IntWritable(sum));
	     }
		 
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Top Five Visitors");
		job.setJarByClass(totalBytes.class);
		job.setMapperClass(byteMapper.class);
		job.setReducerClass(byteReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
