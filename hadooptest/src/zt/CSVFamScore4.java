/*
 * Get the FamScore of each possible structure, given existing parents' child's ID by JobConf. 
 * Mapper output: key->(parentID, value of parents); value->(value of child, times)
 * Reducer output: key->parentID; value-> Score
 */
package zt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.math3.special.Gamma;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Entity.PairIntWritable;

public class CSVFamScore4 extends Configured implements Tool {
	private static int INVALID_STATE = -1;

	// Mapper
	public static class CSVMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, PairIntWritable, PairIntWritable> {
		private int specified;
		private int[] D;
		private int[] given;
		private int[] possible;

		private int encodeGiven(int[] l) {
			int res = 0;
			for (int i : given) {
				int t = l[i];
				if (t == INVALID_STATE)
					return -1;
				res = res * D[i] + t;
			}
			return res;
		}
		private int[] setArray(int length,String values){
			int[] arr=new int[length];
			int p=0;
			for(String s : values.split(",")){
				arr[p++]=Integer.parseInt(s);
			}
			return arr;
		}

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			//csv configure
			D=setArray(job.getInt("csvfamscore.csv.num", -1),job.get("csvfamscore.csv.d"));
//			System.out.println("D: "+D.length+"\t"+D[0]);
			//given part
			given=setArray(job.getInt("csvfamscore.given.num", -1),job.get("csvfamscore.given.value"));
			Arrays.sort(given);
//			System.out.println("given: "+given.length+"\t"+given[0]);
			//possible parents to test
			possible=setArray(job.getInt("csvfamscore.possible.num", -1),job.get("csvfamscore.possible.value"));
			Arrays.sort(possible);
//			System.out.println("possible: "+possible.length+"\t"+possible[0]);
			//specified
			specified = job.getInt("csvfamscore.specified", -1);
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<PairIntWritable, PairIntWritable> output,
				Reporter reporter) throws IOException {
			String[] strs = value.toString().split(",");
			int[] nums=new int[strs.length];
			for(int i=0;i<strs.length;i++)
				nums[i]=Integer.parseInt(strs[i]);
			int oBaseKey,oValue;
			if((oValue=nums[specified])==INVALID_STATE || (oBaseKey=encodeGiven(nums))==INVALID_STATE)
				return;	//get the basic parents' values code
			for(int offset : possible){
				//update parents' values code with "offset" at the end (oBaseKey*D[offset]+nums[offset])
				if(nums[offset]!=INVALID_STATE)
					output.collect(new PairIntWritable(offset,oBaseKey*D[offset]+nums[offset]),
							new PairIntWritable(oValue,1));
			}
		}
	}

	// Combiner:
	public static class CSVCombiner extends MapReduceBase implements
			Reducer<PairIntWritable, PairIntWritable, PairIntWritable, PairIntWritable> {
		private int spfSize;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			int specified=job.getInt("csvfamscore.specified", -1);
			String str = job.get("csvfamscore.csv.d").split(",")[specified];
			spfSize = Integer.parseInt(str);
		}

		@Override
		public void reduce(PairIntWritable key, Iterator<PairIntWritable> values,
				OutputCollector<PairIntWritable, PairIntWritable> output, Reporter reporter) throws IOException {
			int[] res = new int[spfSize];
			while (values.hasNext()) {
				PairIntWritable p = values.next();
				// res[p.getV1()]+=p.getV2();
				res[p.getV1()]++;
			}
			for (int i = 0; i < spfSize; i++) {
				if (res[i] != 0)
					output.collect(key, new PairIntWritable(i, res[i]));
			}
		}
	}

	// Reducer:
	public static class ValReducer extends MapReduceBase implements
			Reducer<PairIntWritable, PairIntWritable, IntWritable, DoubleWritable> {
		private int spfSize;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			String str = job.get("csvfamscore.csv.d");
			List<Integer> D = new ArrayList<Integer>();
			for (String s : str.split(",")) {
				D.add(Integer.parseInt(s));
			}
			spfSize = D.get(job.getInt("csvfamscore.specified", -1));
		}

		@Override
		public void reduce(PairIntWritable key, Iterator<PairIntWritable> values,
				OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
			int[] res = new int[spfSize];
			while (values.hasNext()) {
				PairIntWritable p = values.next();
				// System.out.println(v);
				res[p.getV1()] += p.getV2();
			}
			int sum = 0;
			double score = 0.0;
			for (int i : res) {
				if(i==0)
					continue;
				sum += i;
				score += Gamma.logGamma(1.0 + i);
			}
			score -= Gamma.logGamma(1.0 + sum);
			output.collect(new IntWritable(key.getV1()), new DoubleWritable(score));
		}
	}
	
	public static class FamReducer extends MapReduceBase 
		implements Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
		@Override
		public void reduce(IntWritable key, Iterator<DoubleWritable> values,
				OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
			double res=0.0;
			while(values.hasNext()){
				res+=values.next().get();
			}
			output.collect(key, new DoubleWritable(res));
		}		
	}
 
	// Main Class:
	private String inputPath=null, outputPath=null, tempPath=null;
	
	private void configure(JobConf conf,String[] args) throws Exception{
		int with_conf=0;		
		int specified = 0;
		String csvConfPath=null;
		ArrayList<Integer> given=new ArrayList<Integer>(), possible=new ArrayList<Integer>();
		for (int i = 0; i < args.length; ++i) {
			switch(args[i]){
			case "-csvconf":
				csvConfPath=args[++i];
				with_conf|=1;
				break;
			case "-given":
				for(String s : args[++i].split("\\D")){
					given.add(Integer.parseInt(s));
				}
				with_conf|=2;
				break;
			case "-specified":
				specified=Integer.parseInt(args[++i]);
				with_conf|=4;
				break;
			case "-possible":
				for(String s : args[++i].split("\\D")){
					possible.add(Integer.parseInt(s));
				}
				with_conf|=8;
				break;
			case "-input":
				inputPath=args[++i];
				with_conf|=16;
				break;
			case "-output":
				outputPath=args[++i];
				with_conf|=32;
				break;
			default:
			}
		}
		if (with_conf!=64-1) {
			String str="No enough configuration! (error code="+with_conf+")";
			System.err.println(str);
			throw new Exception(str);
		}
		setConfByConfigure(conf,csvConfPath,given,possible,specified);
	}
	private void setConfByConfigure(JobConf conf,String pCSVConf,
			List<Integer> given,List<Integer> possible, int specified) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		Scanner s = new Scanner(hdfs.open(new Path(pCSVConf)));
		int N = s.nextInt();
		ArrayList<Integer> D = new ArrayList<Integer>();
		for (int i = 0; i < N; i++) {
			D.add(s.nextInt());
		}
		s.close();
		conf.setInt("csvfamscore.csv.num", N);
		conf.set("csvfamscore.csv.d", D.toString().replaceAll("[\\[\\] ]", ""));
		conf.setInt("csvfamscore.given.num", given.size());
		conf.set("csvfamscore.given.value", given.toString().replaceAll("[\\[\\] ]", ""));
		conf.setInt("csvfamscore.possible.num", possible.size());
		conf.set("csvfamscore.possible.value", possible.toString().replaceAll("[\\[\\] ]",""));
		conf.setInt("csvfamscore.specified", specified);
	}
	
	private void set2JobIOPath(JobConf conf1,JobConf conf2){
		int rand=Math.abs(new Random().nextInt());
//		tempPath="TEMP_FAMSCORE_"+System.currentTimeMillis()+"_"+rand;
		tempPath=new String(inputPath);
		tempPath.replaceAll("[^/]+$", "TEMP_FAMSCORE_"+System.currentTimeMillis()+"_"+rand);
		Path p=new Path(tempPath);
		//job1
		FileInputFormat.setInputPaths(conf1, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf1, p);
		//job2
		FileInputFormat.setInputPaths(conf2, p);
		FileOutputFormat.setOutputPath(conf2, new Path(outputPath));		
	}
	private void CleanUp(JobConf conf) throws IOException{
		FileSystem hdfs=FileSystem.get(conf);
		hdfs.delete(new Path(tempPath),true);
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CSVFamScore.class);
		JobConf conf2 = new JobConf(getConf(), CSVFamScore.class);
		conf.setJobName("csv_famscore_front");
		conf2.setJobName("csv_famscore_back");
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapOutputKeyClass(PairIntWritable.class);
		conf.setMapOutputValueClass(PairIntWritable.class);
		conf2.setOutputKeyClass(IntWritable.class);
		conf2.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(CSVMapper.class);
		conf.setCombinerClass(CSVCombiner.class);
		conf.setReducerClass(ValReducer.class);
		conf2.setMapperClass(IdentityMapper.class);
		conf2.setReducerClass(FamReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf2.setInputFormat(SequenceFileInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

//		conf.setNumReduceTasks(2);
		
		configure(conf,args);
		set2JobIOPath(conf,conf2);
			
		JobClient.runJob(conf);//launch 1st job:		
//		System.out.println("Job 1 finished!");
		JobClient.runJob(conf2);//launch 2nd job:
		
		CleanUp(conf);//clean up intermediate files	
		return 0;
	}

	public static void main(String[] args) throws Exception {
		args="-csvconf ../tmp/conf.txt -given 1,4 -possible 2,5,6 -specified 3 -input ../tmp/0.csv -output ../tmp/output7".split(" ");
		int res = ToolRunner.run(new Configuration(), new CSVFamScore(), args);
		System.exit(res);
	}
}
