import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper1 extends Mapper<Object, Text, Text, Text> {

    private static final String CSV_SEP = ",";
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] record = value.toString().split("\\s+");
        String firstHop = record[1];
        String secondHop = record[2];



    }

    }
