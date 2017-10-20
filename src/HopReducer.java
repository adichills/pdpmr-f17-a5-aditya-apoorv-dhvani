// @Author : Apoorv Anand, Aditya Kammardi Sathyanarayan, Dhvani Sheth

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer;

// The reducer then combines all the two hop flights possible between the given 
// origin and destination in input and emits out 
// Key - {Year,Month,Day,ORIG,DEST}, Value - {Firsthop Flight details, Secondhop Flight details} 
// Note: In this step we calculate these output pairs based on the Scheduled Arrival 
// and Departure times.
public class HopReducer extends Reducer<Text, Text, Text, Text> {
    private static final int MINLAYOVERINMINS = 45;
    private static final int MAXLAYOVERINMINS = 720;
    private static final String CSV_SEP = ",";
    private HashMap<String, InputField> validRecordFields = new HashMap<String, InputField>();
    Configuration conf;
    String mode = "";
    private static final String MODE = "crs";

    @Override
    protected void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        String inputfilePath = conf.get("inputFile");
        mode = conf.get("mode");
        populateInputRecords(inputfilePath);
    }

    public String[] getFlightResult(String[] firstHopValues, String[] secondHopValues) {
        String year1 = firstHopValues[1];
        String month1 = firstHopValues[2];
        String day1 = firstHopValues[3];
        String origin1 = firstHopValues[7];
        String destination1 = firstHopValues[8];
        String deptime1 = firstHopValues[4];
        String arrivalTime1 = firstHopValues[5];
        String airline1 = firstHopValues[6];
        String actualArrivalTime1 = firstHopValues[9];
        String cancelled1 = firstHopValues[10];
        String year2 = secondHopValues[1];
        String month2 = secondHopValues[2];
        String day2 = secondHopValues[3];
        String deptime2 = secondHopValues[4];
        String airline2 = secondHopValues[5];
        String origin2 = secondHopValues[6];
        String destination2 = secondHopValues[7];
        String actulDepatureTime2 = secondHopValues[8];
        String cancelled2 = secondHopValues[9];
        String check = year1 + month1 + day1 + origin1 + destination2;
        String[] result = new String[2];
        result[0] = year1 + CSV_SEP + month1 + CSV_SEP + day1 + CSV_SEP
            + deptime1 + CSV_SEP + arrivalTime1 + CSV_SEP + airline1 + CSV_SEP
            + origin1 + CSV_SEP + destination1 +CSV_SEP + actualArrivalTime1 + CSV_SEP
            + cancelled1+ " " + year2 + CSV_SEP + month2 + CSV_SEP + day2 + CSV_SEP 
            + deptime2 + CSV_SEP + airline2 + CSV_SEP + origin2 + CSV_SEP + destination2
            + CSV_SEP + actulDepatureTime2 + CSV_SEP + cancelled2;
        if ((year1.equals(year2) && month1.equals(month2) && day1.equals(day2) && 
            destination1.equals(origin2)) && (validRecordFields.containsKey(check))) {
            result[1] = validRecordFields.get(check).year + CSV_SEP
                + validRecordFields.get(check).month + CSV_SEP
                + validRecordFields.get(check).day + CSV_SEP
                + validRecordFields.get(check).origin + CSV_SEP
                + validRecordFields.get(check).destination;
            return result;
        } else
            return null;
    }

    public void reduce(Text text, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        ArrayList<String> vals1 = new ArrayList<String>();
        for (Text val : values){
            vals1.add(val.toString());
        }
        ArrayList<String> vals2 = new ArrayList<String>();
        vals2.addAll(vals1);
        for (String val1 : vals1) {
            String[] firstHopValues = val1.split(",");
            if (firstHopValues[0].equals("FirstHop")) {
                for (String val2 : vals2) {
                    String[] secondHopValues = val2.split(",");
                    if (secondHopValues[0].equals("SecondHop")) {
                        int arrTimeInMins = CommonUtil.convertIntoMinutes(firstHopValues[5]);
                        int depTimeInMins = CommonUtil.convertIntoMinutes(secondHopValues[4]);
                        int actualArrTimeInMins = CommonUtil.convertIntoMinutes(firstHopValues[9]);
                        int actualDeptTimeInMins = CommonUtil.convertIntoMinutes(secondHopValues[8]);
                        int firstHopCancelled = Integer.parseInt(firstHopValues[10]);
                        int secondHopCancelled = Integer.parseInt(secondHopValues[9]);
                        int deptTimeToConsider = 0;
                        int arrTimeToConsider = 0;
                        if (mode.equals(MODE)) {
                            deptTimeToConsider = depTimeInMins;
                            arrTimeToConsider = arrTimeInMins;
                        }
                        else {
                            deptTimeToConsider = actualDeptTimeInMins;
                            arrTimeToConsider = actualArrTimeInMins;
                        }
                        if (deptTimeToConsider - arrTimeToConsider > MINLAYOVERINMINS
                                && deptTimeToConsider - arrTimeToConsider < MAXLAYOVERINMINS) {
                            String[] result = getFlightResult(firstHopValues,
                                secondHopValues);
                            if (null != result) {
                                //Counter calculation
                                if(actualDeptTimeInMins - actualArrTimeInMins > 
                                    MINLAYOVERINMINS && actualDeptTimeInMins - 
                                    actualArrTimeInMins < MAXLAYOVERINMINS && 
                                    firstHopCancelled == 0 && secondHopCancelled 
                                    == 0) {
                                    context.getCounter(Correctness.CORRECT).increment(1);
                                }
                                else {
                                    context.getCounter(Correctness.INCORRECT).increment(1);
                                }
                                context.write(new Text(result[1]), new Text(
                                    result[0]));
                            }
                        }
                    }
                }
            }
        }
    }

    private void populateInputRecords(String filePath) throws IOException {
        String uri = filePath;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(in, byteArrayOutputStream, 4096, false);
            String fileContents = byteArrayOutputStream.toString();
            String[] records = fileContents.split("\n");
            for (int i = 0; i < records.length; i++) {
                String[] fields = records[i].split(CSV_SEP);
                InputField inputField = new InputField();
                inputField.year = fields[0];
                Integer month = Integer.parseInt(fields[1]);
                inputField.month = month.toString();
                Integer day = Integer.parseInt(fields[2]);
                inputField.day = day.toString();
                inputField.origin = fields[3];
                inputField.destination = fields[4];
                validRecordFields.put(inputField.toString(), inputField);
            }
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
