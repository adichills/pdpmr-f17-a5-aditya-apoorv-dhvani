import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class HopDriver {
    public static void drive(String input, String output) throws IOException,
            ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("inputFile","inputFile/inputs");
        Job hop_calculation = Job.getInstance(conf, "Hop Calculation");

        hop_calculation.setJarByClass(HopDriver.class);
        //hop_calculation.setInputFormatClass(TextInputFormat.class);
        hop_calculation.setOutputFormatClass(TextOutputFormat.class);
        hop_calculation.setReducerClass(HopReducer.class);
        hop_calculation.setNumReduceTasks(1);

        MultipleInputs.addInputPath(hop_calculation, new Path(input),TextInputFormat.class, FirstHopMapper.class);
        MultipleInputs.addInputPath(hop_calculation, new Path(input),TextInputFormat.class, SecondHopMapper.class);

        FileOutputFormat.setOutputPath(hop_calculation, new Path(output));
        hop_calculation.waitForCompletion(true);
    }
}
