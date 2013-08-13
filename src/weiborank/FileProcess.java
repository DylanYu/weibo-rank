package weiborank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mplab2.WordCount;
import mplab2.WordCount.WordCountMapper;
import mplab2.WordCount.WordCountReducer;

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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.csvreader.CsvReader;

public class FileProcess extends Configured implements Tool {

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
			String watchList = reader.get(24 - 4);
			output.collect(type0, ONE);
			if (!watchList.equals(""))
				output.collect(type1, ONE);
		}
	}

	public static class CounterReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		private Text t = new Text("i");


		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
//			System.out.println("reduce() is being called");
			int count = 0;
			while (values.hasNext()) {
				count += values.next().get();
			}
			output.collect(key, new IntWritable(count));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), FileProcess.class);
		conf.setJobName("word count");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(CounterMapper.class);
//		conf.setCombinerClass(CounterReducer.class);
		conf.setReducerClass(CounterReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
//		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf,"/home/helo/weibo_data");
//		FileInputFormat.setInputPaths(conf,"/home/helo/weibo_test");
		FileOutputFormat.setOutputPath(conf, new Path("counter"));
		
		// conf.setOutputFormat(TextOutputFormat.class);
		
		FileSystem.get(conf).delete(new Path("counter"));
		
		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new FileProcess(), args);
		// System.exit(ret);
	}
}