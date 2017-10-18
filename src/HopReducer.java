import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.spec.ECField;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer;

public class HopReducer extends Reducer<Text, Text, Text, Text> {
    private Text joinedResult;
    private static final int MINLAYOVERINMINS = 45;
    private static final int MAXLAYOVERINMINS = 720;
    private static final String CSV_SEP = ",";
    private HashMap<String, InputField> validRecordFields = new HashMap<String, InputField>();
    Configuration conf;

    @Override
    protected void setup(Context context) throws IOException {
        conf = context.getConfiguration();

        String inputfilePath = conf.get("inputFile");

        populateInputLists(inputfilePath);

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

    public String[] getFlightResult(String[] firstHopValues,
            String[] secondHopValues) {
        String year1 = firstHopValues[1];
        String month1 = firstHopValues[2];
        String day1 = firstHopValues[3];
        String origin1 = firstHopValues[7];
        String destination1 = firstHopValues[8];

        String year2 = secondHopValues[1];
        String month2 = secondHopValues[2];
        String day2 = secondHopValues[3];
        String origin2 = secondHopValues[6];
        String destination2 = secondHopValues[7];
        String check = year1 + month1 + day1 + origin1 + destination2;

        String[] result = new String[2];
        result[0] = year1 + CSV_SEP + month1 + CSV_SEP + day1 + CSV_SEP
                + origin1 + CSV_SEP + destination1 + " " + year2 + CSV_SEP
                + month2 + CSV_SEP + day2 + CSV_SEP + origin2 + CSV_SEP
                + destination2;

        if ((year1.equals(year2) && month1.equals(month2) && day1.equals(day2) && destination1
                .equals(origin2))
                && (validRecordFields.containsKey(check))) {
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
        for (Text val1 : values) {
            String[] firstHopValues = val1.toString().split(",");
            if (firstHopValues[0].equals("FirstHop")) {
                for (Text val2 : values) {
                    String[] secondHopValues = val2.toString().split(",");
                    if (secondHopValues[0].equals("SecondHop")) {
                        int arrTimeInMins = convertIntoMinutes(firstHopValues[5]);
                        int depTImeInMins = convertIntoMinutes(secondHopValues[4]);
                        if (depTImeInMins - arrTimeInMins > MINLAYOVERINMINS
                                && depTImeInMins - arrTimeInMins < MAXLAYOVERINMINS) {
                            String[] result = getFlightResult(firstHopValues,
                                    secondHopValues);
                            if (null != result) {
                                context.write(new Text(result[1]), new Text(
                                        result[0]));
                            }
                        }
                    }
                }
            }

        }

    }

    private void populateInputLists(String filePath) throws IOException {
        String uri = filePath;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        StringBuilder sb = new StringBuilder();
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
                inputField.month = fields[1];
                inputField.day = fields[2];
                inputField.origin = fields[3];
                inputField.destination = fields[4];

                validRecordFields.put(inputField.toString(), inputField);
            }

        } finally {
            IOUtils.closeStream(in);
        }

    }
}
