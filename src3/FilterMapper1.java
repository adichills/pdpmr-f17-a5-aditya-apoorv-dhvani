// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper1 extends Mapper<Object, Text, Text, Text> {

    Configuration conf;
    String mapper;
    private  static final String FIRSTHOP = "FirstHop";
    private  static final String SECONDHOP = "SecondHop";
    private  static final String SPACE =  " ";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        mapper = conf.get("mapper");
    }

    private static final String CSV_SEP = ",";
    Text outputKey = new Text();
    Text outputValue = new Text();
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] record = value.toString().split("\\s+");

        if(mapper.equals(FIRSTHOP)){
            StringBuilder sb = new StringBuilder();
            String firstHop = record[1];
            String[] firstHopSplit = firstHop.split(CSV_SEP);
            sb.append(firstHopSplit[1]);
            sb.append(CSV_SEP);
            sb.append(firstHopSplit[5]);
            sb.append(CSV_SEP);
            sb.append(firstHopSplit[6]);
            sb.append(CSV_SEP);
            sb.append(firstHopSplit[7]);

            outputKey.set(sb.toString());
            String valueString = value.toString();
            outputValue.set(FIRSTHOP + SPACE +valueString);
            context.write(outputKey,outputValue);

        }
        else{
            String secondHop = record[2];
            String[] secondHopSplit = secondHop.split(CSV_SEP);
            StringBuilder sb2 = new StringBuilder();
            sb2.append(secondHopSplit[1]);
            sb2.append(CSV_SEP);
            sb2.append(secondHopSplit[4]);
            sb2.append(CSV_SEP);
            sb2.append(secondHopSplit[5]);
            sb2.append(CSV_SEP);
            sb2.append(secondHopSplit[6]);

            outputKey.set(sb2.toString());
            String valueString = value.toString();
            outputValue.set(SECONDHOP+ SPACE + valueString);
            context.write(outputKey,outputValue);

        }


    }

    }
