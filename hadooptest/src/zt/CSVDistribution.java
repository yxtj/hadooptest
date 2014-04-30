/*
 * Get the distribution of a specific column under all possible values of ONE given condition column.
 * In output file: the key is the different state of the given column.  
 */
package zt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Entity.PairIntWritable;
import Entity.VectorIntWritable;

public class CSVDistribution extends Configured implements Tool {
//Mapper
	public static class CSVMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, PairIntWritable> {
		private int given,specified;
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			given=job.getInt("csvdistribution.given",-1);
			specified=job.getInt("csvdistribution.specified", -1);
		}
		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, PairIntWritable> output,
				Reporter reporter) throws IOException {
			String[] strs=value.toString().split(",");
			output.collect(new IntWritable(Integer.parseInt(strs[given])),
					new PairIntWritable(Integer.parseInt(strs[specified]), 1));
		}
	}
//Combiner:
	public static class CSVCombiner extends MapReduceBase implements
			Reducer<IntWritable, PairIntWritable, IntWritable, PairIntWritable> {
		private int spfSize;
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			String str = job.get("csvdistribution.csv.d");
			List<Integer> D=new ArrayList<Integer>();
			for (String s : str.split(",")) {
				D.add(Integer.parseInt(s));
			}
			spfSize=D.get(job.getInt("csvdistribution.specified", -1));
		}
		@Override
		public void reduce(IntWritable key, Iterator<PairIntWritable> values,
				OutputCollector<IntWritable, PairIntWritable> output, Reporter reporter) throws IOException {
			int[] res = new int[spfSize];
			while(values.hasNext()){
				PairIntWritable p=values.next();
				res[p.getV1()]+=p.getV2();
			}
			for(int i=0;i<spfSize;i++){
				output.collect(key, new PairIntWritable(i,res[i]));
			}
		}
	}
//Reducer:
	public static class CSVReducer extends MapReduceBase implements
			Reducer<IntWritable, PairIntWritable, IntWritable, VectorIntWritable> {
		private int spfSize;
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			String str = job.get("csvdistribution.csv.d");
			List<Integer> D=new ArrayList<Integer>();
			for (String s : str.split(",")) {
				D.add(Integer.parseInt(s));
			}
			spfSize=D.get(job.getInt("csvdistribution.specified", -1));		}
		@Override
		public void reduce(IntWritable key, Iterator<PairIntWritable> values,
				OutputCollector<IntWritable, VectorIntWritable> output, Reporter reporter) throws IOException {
			int[] res = new int[spfSize];
			while (values.hasNext()) {
				PairIntWritable p = values.next();
				// System.out.println(v);
				res[p.getV1()]+=p.getV2();
			}
			output.collect(key, new VectorIntWritable(res));
		}
	}
//Main Class:
	private void setConfByConfigure(Path p,int given,int specified,JobConf conf) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		Scanner s = new Scanner(hdfs.open(p));
		int N = s.nextInt();
		ArrayList<Integer> D = new ArrayList<Integer>();
		for (int i = 0; i < N; i++) {
			D.add(s.nextInt());
		}
		s.close();
		conf.setInt("csvdistribution.csv.num", N);
		conf.set("csvdistribution.csv.d", D.toString().replaceAll("[\\[\\] ]", ""));
		conf.setInt("csvdistribution.given", given);
		conf.setInt("csvdistribution.specified", specified);
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CSVDistribution.class);
		conf.setJobName("csv_count");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(VectorIntWritable.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(PairIntWritable.class);

		conf.setMapperClass(CSVMapper.class);
		conf.setCombinerClass(CSVCombiner.class);
		conf.setReducerClass(CSVReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		Path p=null;
		int with_conf = 0;
		int given=0,specified=0; 
		for (int i = 0; i < args.length; ++i) {
			if ("-csvconf".equals(args[i])) {
				// DistributedCache.addCacheFile(new Path(args[++i]).toUri(),conf);
				p = new Path(args[++i]);
				// setConfByDistributedCache(p,conf);
				with_conf|=1;
			}else if("-given".equals(args[i])){
				given=Integer.parseInt(args[++i]);
				with_conf|=2;
			}else if("-specified".equals(args[i])){
				specified=Integer.parseInt(args[++i]);
				with_conf|=4;
			}else {
				other_args.add(args[i]);
			}
		}
		if (with_conf!=7) {
			System.err.println("No enough configure file! (error code="+with_conf+")");
			return 0;
		}
		setConfByConfigure(p,given,specified, conf);

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
		// System.out.println(conf.get("mapred.input.dir"));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CSVDistribution(), args);
		System.exit(res);
	}
}
