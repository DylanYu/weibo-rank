package filter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.csvreader.CsvReader;

/**
 * Count all users and users with watch list.
 * 
 * @author Dongliang Yu
 *
 */
public class UserCounter extends Configured implements Tool {
	public static class CounterMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text all = new Text("All");
		private Text has = new Text("Has");
		private IntWritable ONE = new IntWritable(1);
		private Text outValue = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			 ByteArrayInputStream inputStream = new
			 ByteArrayInputStream(value.toString().getBytes());
			 CsvReader reader = new CsvReader(inputStream,
			 Charset.forName("utf-8"));
			 reader.readRecord();
			 String user = reader.get(0);
			 String watchList = reader.get(20);
			 try {
				 Integer.parseInt(user);
				 output.collect(all, ONE);
				 if (!watchList.equals("")) {
					 output.collect(has, ONE);
				 }
			 }
			 catch (NumberFormatException ex) {
				// Unable to parse UserID, so it's the header, just skip.
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
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), UserCounter.class);
		conf.setJobName("User Counter");
		conf.setJarByClass(UserCounter.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CounterMapper.class);
		 conf.setReducerClass(CounterReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("C:/workspace/weiborank/weibo_data"));
		Path tempDir = new Path("Data - User Counter");
		FileOutputFormat.setOutputPath(conf, tempDir);

		FileSystem.get(conf).delete(tempDir);
		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UserCounter(), args);
	}
}
