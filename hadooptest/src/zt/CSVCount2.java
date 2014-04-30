/*
 * Get the distribution of each column
 */
package zt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

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

public class CSVCount2 extends Configured implements Tool {

	public static class CSVMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, PairIntWritable> {
		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, PairIntWritable> output,
				Reporter reporter) throws IOException {
			int n = 0;
			StringTokenizer tkn = new StringTokenizer(value.toString(), ",");
			while (tkn.hasMoreTokens()) {
				output.collect(new IntWritable(n++), new PairIntWritable(Integer.parseInt(tkn.nextToken()), 1));
			}
		}
	}

	public static class CSVCombiner extends MapReduceBase implements
			Reducer<IntWritable, PairIntWritable, IntWritable, PairIntWritable> {
		private ArrayList<Integer> D = new ArrayList<Integer>();

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			// byDistributedCache(job);
			byConfiguration(job);
		}
		private void byConfiguration(JobConf job) {
			// N=job.getInt("csvcount.conf.num", -1);
			String str = job.get("csvcount.conf.d");
			for (String s : str.split(",")) {
				D.add(Integer.parseInt(s));
			}
		}
		@Override
		public void reduce(IntWritable key, Iterator<PairIntWritable> values,
				OutputCollector<IntWritable, PairIntWritable> output, Reporter reporter) throws IOException {
			int n=D.get(key.get());
			int[] res = new int[n];
//			System.out.println(D+"\n"+n);
//			System.out.println("key="+key);
			while(values.hasNext()){
				PairIntWritable p=values.next();
//				System.out.println(p);
				res[p.getV1()]+=p.getV2();
			}
			for(int i=0;i<n;i++){
				output.collect(key, new PairIntWritable(i,res[i]));
			}
		}
	}

	public static class CSVReducer extends MapReduceBase implements
			Reducer<IntWritable, PairIntWritable, IntWritable, VectorIntWritable> {
		// private int N=0;
		private ArrayList<Integer> D = new ArrayList<Integer>();

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			// byDistributedCache(job);
			byConfiguration(job);
		}

		private void byConfiguration(JobConf job) {
			// N=job.getInt("csvcount.conf.num", -1);
			String str = job.get("csvcount.conf.d");
			for (String s : str.split(",")) {
				D.add(Integer.parseInt(s));
			}
		}

		@Override
		public void reduce(IntWritable key, Iterator<PairIntWritable> values,
				OutputCollector<IntWritable, VectorIntWritable> output, Reporter reporter) throws IOException {
			int[] res = new int[D.get(key.get())];
			// System.out.println(D.get(key.get()));
			while (values.hasNext()) {
				PairIntWritable v = values.next();
				// System.out.println(v);
				res[v.getV1()]+=v.getV2();
			}
			output.collect(key, new VectorIntWritable(res));
		}
	}

	private void setConfByConfigure(Path p, JobConf conf) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		Scanner s = new Scanner(hdfs.open(p));
		int N = s.nextInt();
		ArrayList<Integer> D = new ArrayList<Integer>();
		for (int i = 0; i < N; i++) {
			D.add(s.nextInt());
		}
		s.close();
		conf.setInt("csvcount.conf.num", N);
		conf.set("csvcount.conf.d", D.toString().replaceAll("[\\[\\] ]", ""));
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CSVCount2.class);
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
		boolean with_conf = false;
		for (int i = 0; i < args.length; ++i) {
			if ("-csvconf".equals(args[i])) {
				// DistributedCache.addCacheFile(new Path(args[++i]).toUri(),
				// conf);
				Path p = new Path(args[++i]);
				// setConfByDistributedCache(p,conf);
				setConfByConfigure(p, conf);
				with_conf = true;
			} else {
				other_args.add(args[i]);
			}
		}
		if (!with_conf) {
			System.err.println("No configure file!");
			return 0;
		}

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
		// System.out.println(conf.get("mapred.input.dir"));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CSVCount2(), args);
		System.exit(res);
	}
}
