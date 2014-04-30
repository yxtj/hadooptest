package test.hadoop;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobConfInfo extends Configured implements Tool {

	private void output(Path out,JobConf conf){
		PrintWriter output=null;
		try{
			output=new PrintWriter(new BufferedWriter(new FileWriter(out.toString())));
		}catch(IOException e){
			System.out.println("Cannot open output file: " + out.toString() + " !");
			e.printStackTrace();
			return;
		}
		int n=conf.getInt("num",0);
		output.println(n);
		output.println(out.toString());
		output.println(out.toUri().toString());
		output.println("list1:"+conf.get("list1"));
		output.println("list2:"+conf.get("list2"));
		output.println("list3:"+conf.get("list3"));
		output.println("list2:");
		StringTokenizer tkn=new StringTokenizer(conf.get("list2"),",");
		while(tkn.hasMoreTokens()){
			output.println(tkn.nextToken());
		}
		output.println("list3:");
		String str[]=conf.getStrings("list3");
		for(String s : str){
			output.println(s);
		}
		output.close();
	}
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(false);
		List<Integer> l = new ArrayList<Integer>();
		for (int i = 0; i < 5; i++) {
			l.add(i);
		}
		conf.setInt("num", l.size());
		conf.set("list1", l.toString());
		conf.set("list2", l.toString().replaceAll("[\\[\\] ]", ""));
		conf.setStrings("list3", l.get(0).toString(), l.get(1).toString(), l.get(2).toString(), l.get(3).toString(), l
				.get(4).toString());
		
		Path out=new Path(args[0].toString());
		System.out.println("Start outputing");
		output(out,conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new JobConfInfo(), args);
		System.exit(res);
	}
}
