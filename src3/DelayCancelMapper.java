// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

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

        CSVRecord record = new CSVRecord(value.toString());
        if(CommonUtil.cleanData(record)){
            if (record.get(0).compareTo(inputYear) < 0) {
                StringBuilder sb = new StringBuilder();
                sb.append(record.get(2));
                sb.append(CSV_SEP);
                sb.append(record.get(6).replaceAll("\\s+", ""));
                sb.append(CSV_SEP);
                sb.append(record.get(14));
                sb.append(CSV_SEP);
                sb.append(record.get(23));
                outputKey.set(sb.toString());
                String output = (record.get(47).equals("1"))? record.get(43) + CSV_SEP + "1": record.get(43) + CSV_SEP + "0";
                outputValue.set(output);
                context.write(outputKey,outputValue);
            }
        }
    }
}
