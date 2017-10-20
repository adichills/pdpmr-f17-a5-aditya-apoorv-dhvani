// @Author : Apoorv Anand, Aditya Kammardi Sathyanarayan, Dhvani Sheth

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

// this reducer writes the predicted flight hops as the output taking into 
// consideration the mean delay and cancellation factor
// input key - month, airline, origin, destination
// input value - either output from FilterMapper1 or FilterMapper2
// output key - year, month, day, origin, destination 
// output value - First hop flight, Second hop flight
public class FilterReducer extends Reducer<Text, Text, Text, Text> {
    Configuration conf;
    String mapper;
    private  static final String FIRSTHOP = "FirstHop";
    private static final int MINLAYOVERINMINS = 45;
    private static final int MAXLAYOVERINMINS = 720;
    Text outputKey = new Text();
    Text outputValue = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        mapper = conf.get("mapper");
    }

    public void reduce(Text text, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        ArrayList<String> vals1 = new ArrayList<String>();
        String delayAndCancelledRecord;
        String meanDelay = "0";
        String cancelledFraction = "0.0";
        for (Text val : values) {
            if(val.toString().split("\\s+")[0].equals("DelayAndCancelled")) {
                delayAndCancelledRecord = val.toString();
                String[] delayAndCancelledRecordSplit =  delayAndCancelledRecord.
                    split("\\s+");
                String [] record  = delayAndCancelledRecordSplit[1].split(",");
                meanDelay = record[0];
                cancelledFraction = record[1];
            }
            else {
                vals1.add(val.toString());
            }
        }

        if (mapper.equals(FIRSTHOP)) {
            for(String val:vals1){
                String [] valSplit = val.split("\\s+");
                String firsthop = valSplit[2];
                String secondHop = valSplit[3];
                String[] firsthopRecordSplit = firsthop.split(",");
                String[] secondHopRecordSplit = secondHop.split(",");
                // initial flag value true
                boolean flag = true;
                int arrTimeInMinutes = CommonUtil.convertIntoMinutes(firsthopRecordSplit[4]);
                int depTimeInMinutes = CommonUtil.convertIntoMinutes(secondHopRecordSplit[3]);
                int actualArrTimeInMins = CommonUtil.convertIntoMinutes(firsthopRecordSplit[8]);
                int actualDeptTimeInMins = CommonUtil.convertIntoMinutes(secondHopRecordSplit[7]);
                int firstHopCancelled = Integer.parseInt(firsthopRecordSplit[9]);
                int secondHopCancelled = Integer.parseInt(secondHopRecordSplit[8]);
                int meanDelayInt = Integer.parseInt(meanDelay);
                // if the layover is less than 45 minutes, set the flag as false
                // and don't write this flight hops to the output
                if ((depTimeInMinutes - (arrTimeInMinutes + meanDelayInt)) < 45) {
                    flag = false;
                }
                // check for cancellation factor. if it is greater than 0.2, set
                // the flag as false and don't write this flight hops to the
                // output
                if (flag) {
                    Double cfDouble = Double.parseDouble(cancelledFraction);
                    if (cfDouble > 0.2){
                        flag = false;
                    }
                }
                if (flag) {
                    // Counter calculation - if the difference between actual 
                    // arrival time of first hop flight and actual departure time 
                    // of second hop flight has 45 min layover and neither of the
                    // two flights are cancelled, then increment the correctness
                    // counter by 1; else increment incorrect counter by 1.
                    if (actualDeptTimeInMins - actualArrTimeInMins > MINLAYOVERINMINS &&
                        actualDeptTimeInMins - actualArrTimeInMins < MAXLAYOVERINMINS
                        && firstHopCancelled == 0 && secondHopCancelled == 0) {
                        context.getCounter(Correctness.CORRECT).increment(1);
                    }
                    else {
                        context.getCounter(Correctness.INCORRECT).increment(1);
                    }
                    outputKey.set(valSplit[1]);
                    outputValue.set(firsthop + " " + secondHop);
                    context.write(outputKey,outputValue);
                }
            }
        }
        else {
            for (String val:vals1) {
                String [] valSplit = val.split("\\s+");
                String firsthop = valSplit[2];
                String secondHop = valSplit[3];
                String[] firsthopRecordSplit = firsthop.split(",");
                String[] secondHopRecordSplit = secondHop.split(",");
                int actualArrTimeInMins = CommonUtil.convertIntoMinutes(firsthopRecordSplit[8]);
                int actualDeptTimeInMins = CommonUtil.convertIntoMinutes(secondHopRecordSplit[7]);
                int firstHopCancelled = Integer.parseInt(firsthopRecordSplit[9]);
                int secondHopCancelled = Integer.parseInt(secondHopRecordSplit[8]);
                boolean flag = true;
                Double cfDouble = Double.parseDouble(cancelledFraction);
                if (cfDouble > 0.2) { 
                    flag = false;
                }
                if (flag) {
                    if(actualDeptTimeInMins - actualArrTimeInMins > MINLAYOVERINMINS &&
                        actualDeptTimeInMins - actualArrTimeInMins < MAXLAYOVERINMINS
                        && firstHopCancelled == 0
                        && secondHopCancelled == 0){
                        context.getCounter(Correctness.CORRECT).increment(1);
                    }
                    else {
                        context.getCounter(Correctness.INCORRECT).increment(1);
                    }
                    outputKey.set(valSplit[1]);
                    outputValue.set(valSplit[2] + " " + secondHop);
                    context.write(outputKey,outputValue);
                }
            }
        }
    }
}
