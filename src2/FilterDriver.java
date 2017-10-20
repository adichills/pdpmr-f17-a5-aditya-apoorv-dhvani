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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterDriver {
    public static List<String> drive(String filterType, String hopInput, String meanDelayAndCancelledInput, String output) throws IOException,
            ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();
        conf.set("mapper",filterType);

        Job job = Job.getInstance(conf, "Filter " + filterType);

        job.setJarByClass(FilterDriver.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setMapperClass(DelayCancelMapper.class);
        //job.setInputFormatClass(TextInputFormat.class);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);

        MultipleInputs.addInputPath(job,new Path(hopInput),TextInputFormat.class,FilterMapper1.class);
        MultipleInputs.addInputPath(job,new Path(meanDelayAndCancelledInput),TextInputFormat.class,FilterMapper2.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);


        Counters counters = job.getCounters();
        Counter correct = counters.findCounter(CORRECTNESS.CORRECT);
        System.out.println(correct.getDisplayName() + " " + correct.getValue());

        Counter incorrect = counters.findCounter(CORRECTNESS.INCORRECT);
        System.out.println(incorrect.getDisplayName() + " " + incorrect.getValue());

        List<String> list = new ArrayList<String>();
        list.add(String.valueOf(correct.getValue()));
        list.add(String.valueOf(incorrect.getValue()));
        return list;

    }
}
