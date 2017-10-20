// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CleanDataDriver{
    public static void drive(String input, String output) throws IOException,
        ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("input_path", input);
        Job cleanData = Job.getInstance(conf, "Clean Data");
        cleanData.setJarByClass(CleanDataDriver.class);
        cleanData.setMapperClass(CleanDataMapper.class);
        cleanData.setOutputKeyClass(NullWritable.class);
        cleanData.setOutputValueClass(Text.class);
        cleanData.setInputFormatClass(TextInputFormat.class);
        cleanData.setOutputFormatClass(TextOutputFormat.class);
        //no reducers ,as we only intend to clean the data
        cleanData.setNumReduceTasks(0);
        // Delete output file if it already exists
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(cleanData, new Path(input));
        FileOutputFormat.setOutputPath(cleanData, new Path(output));
        cleanData.waitForCompletion(true);
    }
}