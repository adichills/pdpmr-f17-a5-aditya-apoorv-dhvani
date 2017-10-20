import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class HopDriver {
    public static void drive(String flightData,String mode,String input, String output) throws IOException,
            ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("inputFile","inputFile/inputs");
        conf.set("mode",mode);
        Job hop_calculation = Job.getInstance(conf, "Hop Calculation");

        hop_calculation.setJarByClass(HopDriver.class);
        hop_calculation.setReducerClass(HopReducer.class);
        hop_calculation.setOutputKeyClass(Text.class);
        hop_calculation.setOutputValueClass(Text.class);
        hop_calculation.setMapperClass(HopMapper.class);
        hop_calculation.setInputFormatClass(TextInputFormat.class);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        //hop_calculation.setNumReduceTasks(0);

        //MultipleInputs.addInputPath(hop_calculation, new Path(input),TextInputFormat.class, FirstHopMapper.class);
        //MultipleInputs.addInputPath(hop_calculation, new Path("input_sample_2"),TextInputFormat.class, SecondHopMapper.class);
        FileInputFormat.addInputPath(hop_calculation,new Path(flightData));
        FileOutputFormat.setOutputPath(hop_calculation, new Path(output));
        hop_calculation.waitForCompletion(true);

        //get counters
        Counters counters = hop_calculation.getCounters();
        Counter correct = counters.findCounter(CORRECTNESS.CORRECT);
        System.out.println(correct.getDisplayName() + " " + correct.getValue());

        Counter incorrect = counters.findCounter(CORRECTNESS.INCORRECT);
        System.out.println(incorrect.getDisplayName() + " " + incorrect.getValue());
    }
}
