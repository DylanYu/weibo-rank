package filter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
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

/**
 * KNN algorithm used to filter inactive users
 * 
 * @author Dongliang Yu
 *
 */
public class KNN  extends Configured implements Tool {
	public static class KNNMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		private Path cacheFile;
		
//		Properties cache;
		private BufferedReader br;
		
		@Override
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				Path[] urlFile = DistributedCache.getLocalCacheFiles(job);
				cacheFile = urlFile[0];
				br = new BufferedReader(new FileReader(cacheFile.toString()));
				System.out.println(br.readLine());
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// System.out.println("map() is being called");
//			String s = value.toString();
//			String ss = s.replaceAll(otherPattern, " ");
//			StringTokenizer tokenizer = new StringTokenizer(ss);
//			String w;
//			while (tokenizer.hasMoreTokens()) {
//				w = tokenizer.nextToken();
//				if (!patternsToSkip.contains(w)) {
//					word.set(w);
//					output.collect(word, ONE);
//				}
//
//			}
		}
	}

	public static class KNNReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// System.out.println("reduce() is being called");
//			int wc = 0;
//			while (values.hasNext()) {
//				wc += values.next().get();
//			}
//			if (wc > threshold) {
//				output.collect(key, new IntWritable(wc));
//			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), KNN.class);
		conf.setJobName("KNN");
		conf.setJarByClass(KNN.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(KNNMapper.class);
//		conf.setReducerClass(KNNReducer.class);
//		conf.setReducerClass(KNNReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path("C:/workspace/weiborank/weibo_test"));
		Path tempDir = new Path("knn");
		// Path tempDir = new Path("output");
		FileOutputFormat.setOutputPath(conf, tempDir);
		// conf.setOutputFormat(TextOutputFormat.class);
		
		DistributedCache.addCacheFile(URI.create("file:///C:/workspace/weiborank/cache.csv"), conf);
		JobClient.runJob(conf);

		FileSystem.get(conf).delete(tempDir);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KNN(), args);
		// for (String s: args)
		// System.out.println(s);

	}
}
