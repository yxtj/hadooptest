package test.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfInfo extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		private int N;
		private ArrayList<Integer> D=new ArrayList<Integer>();
		private int count=0;
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			output.collect(new IntWritable(count++), new Text(N+" : "+D.toString()));
		}

		@Override
		public void configure(JobConf job) {
			System.out.println("start super configure");
			super.configure(job);
			System.out.println("start custom configure");
			N=job.getInt("num", 0);
			StringTokenizer tkn=new StringTokenizer(job.get("array"),",");
			while(tkn.hasMoreTokens()){
				D.add(Integer.parseInt(tkn.nextToken()));
			}
			System.out.println("finish load N and D. size="+D.size());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), ConfInfo.class);
		conf.setJobName("conf_info_in_map");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(IdentityReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setInputFormat(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setInt("num", 3);
		conf.set("array","4,7,9");
		
		JobClient.runJob(conf);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ConfInfo(), args);
		System.exit(res);
	}

}
