// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class MeanDelayDriver {
    public static void drive(String input, String output, String filterBy, 
        String top5RecordsFile) throws Exception {
        Configuration conf = new Configuration();
        // Read output from topAirline
        String records = FileUtil.top5ActiveRecords(top5RecordsFile);
        conf.set("top5Records",records);
        conf.set("filterBy",filterBy);
        Job job = Job.getInstance(conf, "Mean delay count");
        job.setJarByClass(MeanDelayDriver.class);
        job.setMapperClass(MeanDelayMapper.class);
        job.setReducerClass(MeanDelayReducer.class);
        job.setMapOutputKeyClass(MeanDelayCompositeKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        // Delete output file if it already exists
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setPartitionerClass(SecondarySort.KeyPartitioner.class);
        job.setGroupingComparatorClass(SecondarySort.GroupComparator.class);
        job.setSortComparatorClass(SecondarySort.KeyComparator.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }
}
