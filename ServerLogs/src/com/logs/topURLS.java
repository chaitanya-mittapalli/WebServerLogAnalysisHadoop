package com.logs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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

public class topURLS {

	public static class TopFiveMapper extends Mapper<Object, Text, Text, IntWritable> {
		// Our output key and value Writables
		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			// Parse the input string into a nice map
			/*StringTokenizer stringTokenizer=new StringTokenizer(value.toString()," ");
			String visitor=stringTokenizer.nextToken();*/
		/*	StringTokenizer stringTokenizer2=new StringTokenizer(value.toString()," ");
        	String bytes="";
        	while(stringTokenizer2.hasMoreTokens())
        	  {
        		bytes=stringTokenizer2.nextToken();
        	  }*/
			try
			{
        	int index=value.toString().indexOf("\"");
        	int lastIndex=value.toString().lastIndexOf("\"");
        	String httpMessage=value.toString().substring(index+1, lastIndex);
           	StringTokenizer stringTokenizer3=new StringTokenizer(httpMessage," ");
        	stringTokenizer3.nextToken();
        	String requestedURL=stringTokenizer3.nextToken(); 
        	context.write(new Text(requestedURL), one);
			}
			catch (Exception e) 
			{
				// TODO: handle exception
			}
		}

		
	}

	public static class TopFiveReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			 	int sum = 0;
	            for (IntWritable val : values)
	            {
	                sum += val.get();
	            }
	           countMap.put(new Text(key),new IntWritable(sum));
		}
		 @Override
	      protected void cleanup(Context context) throws IOException, InterruptedException 
		 { 
			 Set<Entry<Text, IntWritable>> set = countMap.entrySet();
		        List<Entry<Text, IntWritable>> list = new ArrayList<Entry<Text, IntWritable>>(set);
		        Collections.sort( list, new Comparator<Map.Entry<Text, IntWritable>>()
		        {
		            public int compare( Map.Entry<Text, IntWritable> o1, Map.Entry<Text, IntWritable> o2 )
		            {
		                return (o2.getValue()).compareTo( o1.getValue() );
		            }
		        } );
		        int counter=0;
		        for(Map.Entry<Text, IntWritable> entry:list)
		        {
		        	counter++;
		        	if(counter>5)break;
		        	context.write(entry.getKey(),entry.getValue());
		        	
		        }
	
			 
	     }
		 
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Top Five URL's Visited");
		job.setJarByClass(topURLS.class);
		job.setMapperClass(TopFiveMapper.class);
		job.setReducerClass(TopFiveReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

