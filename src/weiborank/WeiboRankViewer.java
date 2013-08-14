/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weiborank;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Qian Yu
 */
public class WeiboRankViewer extends Configured implements Tool {
    
    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split("\t");
            if(content.length > 1) {
                String[] subContent = content[1].split("%");
                double prValue = Double.parseDouble(subContent[0]);
                context.write(new DoubleWritable(prValue), new Text(content[0]));
            }
        }
        
    }
    

    
    private static class DecDoubleWritable extends DoubleWritable.Comparator {  
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
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);
        Job job = new Job(conf, "PageRankViewer");
        job.setJarByClass(WeiboRankViewer.class);
        job.setMapperClass(Map.class); 
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(DecDoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WeiboRankViewer(), args);
        System.exit(ret);
    }

}
