import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayCancelMapper extends Mapper<Object, Text, Text, Text> {

    Configuration conf;
    String inputYear;
    private static final String CSV_SEP = ",";
    Text outputKey = new Text();
    Text outputValue = new Text();

    protected void setup(Context context) throws IOException {
        conf = context.getConfiguration();

        inputYear = conf.get("inputYear");



    }

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] record = value.toString().split(CSV_SEP);

        if (record[0].compareTo(inputYear)<0){
               StringBuilder sb = new StringBuilder();
               sb.append(record[2]);
               sb.append(CSV_SEP);
               sb.append(record[6]);
               sb.append(CSV_SEP);
               sb.append(record[13]);
               sb.append(CSV_SEP);
               sb.append(record[22]);
               outputKey.set(sb.toString());

               String output = (record[42].equals("1"))? record[38] + CSV_SEP + "1": record[38] + CSV_SEP + "0";

               outputValue.set(output);
               context.write(outputKey,outputValue);
        }



    }




    }
