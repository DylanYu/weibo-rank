package weiborank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeiboRanklter extends Configured implements Tool {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split("\t");
            String url = content[0];
            String[] subContent = content[1].split("%");
            if(subContent.length > 1) {
                double rankValue = Double.parseDouble(subContent[0]);
                String[] link = subContent[1].split(",");
                for(int i = 0; i < link.length; i++) {
                    String tmp = ":" + Double.toString(rankValue / link.length);
                    context.write(new Text(link[i]), new Text(tmp));
                }
                context.write(new Text(url), new Text(subContent[1]));
            }
        }
        
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        
        private static final double d = 0.85;
//        private static final double minRank = (1 - d) / 1000000;
        private static final double minRank = 1 - d;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String value = "";
            String list = "";
            double eleValue;
            double rank = 0.0;
            Iterator<Text> it = values.iterator();
            while(it.hasNext()) {
                value = it.next().toString();
                if(value.substring(0, 1).equals(":")) {
                    eleValue = Double.parseDouble(value.substring(1));
                    rank += eleValue;
                } else {
                    list = value;
                }
            }
            rank = minRank + d * rank;
            String content = Double.toString(rank) + "%" + list;
            context.write(key, new Text(content));
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);
        Job job = new Job(conf, "PageRanklter");
        job.setJarByClass(WeiboRanklter.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WeiboRanklter(), args);
    }
}
