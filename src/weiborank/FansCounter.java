package weiborank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mplab2.WordCount.WordCountMapper;
import mplab2.WordCount.WordCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.csvreader.CsvReader;

public class FansCounter extends Configured implements Tool {

	public static class CounterMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text type0 = new Text("all");
		private Text type1 = new Text("have");
		private Text missile = new Text();

		private IntWritable ONE = new IntWritable(1);

		@Override
		public void configure(JobConf job) {

		}
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			ByteArrayInputStream inputStream = new ByteArrayInputStream(value.toString().getBytes());
			CsvReader reader = new CsvReader(inputStream, Charset.forName("utf-8"));
			reader.readRecord();
			String user = reader.get(0);
			String[] watchList = reader.get(24 - 4).split(",");
			try {
				int userID = Integer.parseInt(user);
				for (String watched: watchList) {
					missile.set(watched);
					output.collect(missile, ONE);
				}
			}
			catch (NumberFormatException ex) {
				// Can't parse UserID, so it's the header, just skip.
				;
			}
			
		}
	}

	public static class CounterReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while (values.hasNext()) {
				count += values.next().get();
			}
			output.collect(key, new IntWritable(count));
		}
	}

	public static class InverseOutputReducer extends MapReduceBase implements
			Reducer<IntWritable, Text, Text, IntWritable>{

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			List<String> list = new ArrayList<String>();
			while (values.hasNext()) {
				list.add(values.next().toString());
			}

			Collections.sort(list);
			for (String str : list) {
				output.collect(new Text(str), key);
			}
			
		}
		
	}
	
	private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), FansCounter.class);
		conf.setJobName("USER_COUNT");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CounterMapper.class);
//		conf.setCombinerClass(CounterReducer.class);
		conf.setReducerClass(CounterReducer.class);

		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(conf,"/home/helo/weibo_data");
//		FileInputFormat.setInputPaths(conf,"/home/helo/weibo_test");
//		FileInputFormat.setInputPaths(conf,"C:/Yu/weibo_test");
//		FileInputFormat.setInputPaths(conf,"C:/Yu/weibo_data");
//		FileOutputFormat.setOutputPath(conf, new Path("counter"));

		Path tempDir = new Path("UserCount_temp"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileOutputFormat.setOutputPath(conf, tempDir);
		
		JobClient.runJob(conf);

		
		JobConf sortJob = new JobConf(getConf(), FansCounter.class);
		sortJob.setJobName("SORT_JOB");

//		sortJob.setInputFormat(TextInputFormat.class);
		sortJob.setInputFormat(SequenceFileInputFormat.class);
		sortJob.setOutputFormat(TextOutputFormat.class);
		sortJob.setOutputKeyClass(IntWritable.class);
		sortJob.setOutputValueClass(Text.class);
		
		sortJob.setMapperClass(InverseMapper.class);
		sortJob.setReducerClass(InverseOutputReducer.class);
		
		sortJob.setNumReduceTasks(1);
		sortJob.setOutputKeyComparatorClass(IntWritableDecreasingComparator.class);
		
		Path resultPath = new Path("FansRank");
		
		FileInputFormat.setInputPaths(sortJob, tempDir);
		FileOutputFormat.setOutputPath(sortJob, resultPath);
		
		FileSystem.get(sortJob).delete(resultPath);
		
		JobClient.runJob(sortJob);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new FansCounter(), args);
	}
}