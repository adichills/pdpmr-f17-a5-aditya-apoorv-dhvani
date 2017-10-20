// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

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

    private int convertIntoMinutes(String time) {
        try {
            if (time.length() < 3 || time.length() > 4)
                return -1;
            else if (time.length() == 3) {
                int hours = Integer.parseInt("" + time.charAt(0));
                int minutes = Integer.parseInt("" + time.charAt(1)
                        + time.charAt(2));
                return hours * 60 + minutes;
            } else {
                int hours = Integer.parseInt("" + time.charAt(0)
                        + time.charAt(1));
                int minutes = Integer.parseInt("" + time.charAt(2)
                        + time.charAt(3));
                return hours * 60 + minutes;
            }
        } catch (NumberFormatException e) {
            return -1;
        }
    }


    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<String> vals1 = new ArrayList<String>();
        String delayAndCancelledRecord;
        String meanDelay = "0";
        String cancelledFraction = "0.0";

        for (Text val : values){
            if(val.toString().split("\\s+")[0].equals("DelayAndCancelled")){
                delayAndCancelledRecord = val.toString();
                String[] delayAndCancelledRecordSplit =  delayAndCancelledRecord.split("\\s+");
                String [] record  = delayAndCancelledRecordSplit[1].split(",");
                meanDelay = record[0];
                cancelledFraction = record[1];
            }
            else{
                vals1.add(val.toString());
            }

        }

        if(mapper.equals(FIRSTHOP)){
            for(String val:vals1){
                String [] valSplit = val.split("\\s+");
                String firsthop = valSplit[2];
                String secondHop = valSplit[3];

                String[] firsthopRecordSplit = firsthop.split(",");
                String[] secondHopRecordSplit = secondHop.split(",");

                boolean flag = true;
                int arrTimeInMinutes = convertIntoMinutes(firsthopRecordSplit[4]);
                int depTimeInMinutes = convertIntoMinutes(secondHopRecordSplit[3]);

                int actualArrTimeInMins = convertIntoMinutes(firsthopRecordSplit[8]);
                int actualDeptTimeInMins = convertIntoMinutes(secondHopRecordSplit[7]);

                int firstHopCancelled = Integer.parseInt(firsthopRecordSplit[9]);
                int secondHopCancelled = Integer.parseInt(secondHopRecordSplit[8]);



                int meanDelayInt = Integer.parseInt(meanDelay);
                if ((depTimeInMinutes - (arrTimeInMinutes+meanDelayInt))<45){
                    flag = false;
                }
                if(flag){
                    Double cfDouble = Double.parseDouble(cancelledFraction);
                    if (cfDouble>0.2){
                        flag = false;
                    }
                }
                if(flag){

                    //Counter calculation

                    if(actualDeptTimeInMins -actualArrTimeInMins > MINLAYOVERINMINS &&
                            actualDeptTimeInMins - actualArrTimeInMins < MAXLAYOVERINMINS
                            && firstHopCancelled==0
                            && secondHopCancelled==0){
                        context.getCounter(CORRECTNESS.CORRECT).increment(1);
                    }
                    else{
                        context.getCounter(CORRECTNESS.INCORRECT).increment(1);
                    }


                    outputKey.set(valSplit[1]);
                    outputValue.set(firsthop + " " + secondHop);
                    context.write(outputKey,outputValue);
                }

            }
        }
        else{
            for(String val:vals1){
                String [] valSplit = val.split("\\s+");
                String firsthop = valSplit[2];
                String secondHop = valSplit[3];
                String[] firsthopRecordSplit = firsthop.split(",");
                String[] secondHopRecordSplit = secondHop.split(",");

                int arrTimeInMinutes = convertIntoMinutes(firsthopRecordSplit[4]);
                int depTimeInMinutes = convertIntoMinutes(secondHopRecordSplit[3]);

                int actualArrTimeInMins = convertIntoMinutes(firsthopRecordSplit[8]);
                int actualDeptTimeInMins = convertIntoMinutes(secondHopRecordSplit[7]);

                int firstHopCancelled = Integer.parseInt(firsthopRecordSplit[9]);
                int secondHopCancelled = Integer.parseInt(secondHopRecordSplit[8]);


                boolean flag = true;
                Double cfDouble = Double.parseDouble(cancelledFraction);
                if (cfDouble>0.2){
                    flag = false;
                }
                if(flag){

                    if(actualDeptTimeInMins -actualArrTimeInMins > MINLAYOVERINMINS &&
                            actualDeptTimeInMins - actualArrTimeInMins < MAXLAYOVERINMINS
                            && firstHopCancelled==0
                            && secondHopCancelled==0){
                        context.getCounter(CORRECTNESS.CORRECT).increment(1);
                    }
                    else{
                        context.getCounter(CORRECTNESS.INCORRECT).increment(1);
                    }


                    outputKey.set(valSplit[1]);
                    outputValue.set(valSplit[2] + " " + secondHop);
                    context.write(outputKey,outputValue);
                }
            }

        }



    }
}
