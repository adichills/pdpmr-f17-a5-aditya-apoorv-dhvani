// @Author : Apoorv Anand, Aditya Kammardi Sathyanarayan, Dhvani Sheth

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// calculate the mean delay and ratio of cancelled flights (number of flights
// cancelled/total number of flights) per month, airline, origin anad destination 
public class DelayCancelReducer extends Reducer<Text, Text, Text, Text> {
    Text outputValue = new Text();
    private static final String CSV_SEP = ",";
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        Double sumDelays = 0.0;
        int total = 0;
        Integer totalCancelled = 0;
        for (Text value:values) {
            String [] results = value.toString().split(CSV_SEP);
            Double delay = Double.parseDouble(results[0]);
            sumDelays += delay;
            if (results[1].equals("1")) {
                totalCancelled+=1;
            }
            total+=1;
        }
        Double meanDelay = 0.0;
        Double cancelledFlightsFrac = 0.0;
        if (total!=0) {
            meanDelay = sumDelays/total;
            cancelledFlightsFrac = totalCancelled*1.0/total;
        }
        int roundedMeanDelay = meanDelay.intValue();
        roundedMeanDelay++;
        outputValue.set(roundedMeanDelay + CSV_SEP + cancelledFlightsFrac);
        context.write(key,outputValue);
    }
}
