// @Author : Apoorv Anand, Aditya Kammardi Sathyanarayan, Dhvani Sheth

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// driver class for HopMapper and HopReducer
public class HopDriver {
    public static List<String> drive(String flightData, String mode, String input, 
        String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("inputFile", "inputFile/inputs");
        conf.set("mode", mode);
        Job hop_calculation = Job.getInstance(conf, "Hop Calculation");
        hop_calculation.setJarByClass(HopDriver.class);
        hop_calculation.setReducerClass(HopReducer.class);
        hop_calculation.setOutputKeyClass(Text.class);
        hop_calculation.setOutputValueClass(Text.class);
        hop_calculation.setMapperClass(HopMapper.class);
        hop_calculation.setInputFormatClass(TextInputFormat.class);
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(hop_calculation, new Path(flightData));
        FileOutputFormat.setOutputPath(hop_calculation, new Path(output));
        hop_calculation.waitForCompletion(true);
        //get counters
        Counters counters = hop_calculation.getCounters();
        Counter correct = counters.findCounter(Correctness.CORRECT);
        System.out.println(correct.getDisplayName() + " " + correct.getValue());
        Counter incorrect = counters.findCounter(Correctness.INCORRECT);
        System.out.println(incorrect.getDisplayName() + " " + incorrect.getValue());
        List<String> list = new ArrayList<String>();
        list.add(String.valueOf(correct.getValue()));
        list.add(String.valueOf(incorrect.getValue()));
        return list;
    }
}
