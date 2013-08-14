/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weiborank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.csvreader.CsvReader;

public class GraphBuilder extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValye = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	ByteArrayInputStream inputStream = new ByteArrayInputStream(value.toString().getBytes());
			CsvReader reader = new CsvReader(inputStream, Charset.forName("utf-8"));
			reader.readRecord();
			String user = reader.get(0);
			String watchList = reader.get(20);
			if (watchList.equals("")) {
				// Has no watch list, just skip
				;
			}
			else {
				try {
					Integer.parseInt(user);
					outputKey.set(user);
					String rankAndWatchList = "1%" + watchList;
					outputValye.set(rankAndWatchList);
					context.write(outputKey, outputValye);
				}
				catch (NumberFormatException ex) {
					// Can't parse UserID, so it's the header, just skip.
					;
				}
			}
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);
        Job job = new Job(conf, "GraphBuilder");
        job.setJarByClass(GraphBuilder.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new GraphBuilder(), args);
    }
}
