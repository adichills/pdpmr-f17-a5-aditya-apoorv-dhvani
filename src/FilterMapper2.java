// @Author : Apoorv Anand, Aditya Kammardi Sathyanarayan, Dhvani Sheth

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// this mapper reads the output from DelayCancelReducer which has mean delay and
// cancellation factor per month, airline, origin and destination
// output key - month, day, origin, destination 
// output value - mean delay, cancellation factor
public class FilterMapper2 extends Mapper<Object, Text, Text, Text> {
    Text outputKey = new Text();
    Text outputValue = new Text();
    public void map (Object key, Text value, Context context) throws IOException,
        InterruptedException {
        String[] record = value.toString().split("\\s+");
        String record1 = record[0];
        String record2 = record[1];
        outputKey.set(record1);
        outputValue.set("DelayAndCancelled" + " " + record2);
        context.write(outputKey,outputValue);
    }
}