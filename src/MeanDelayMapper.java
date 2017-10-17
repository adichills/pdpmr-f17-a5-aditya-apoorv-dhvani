// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// calculate the normalized delay and output that as value and key as defined in
// MeanDelayCompositeKey. 
public class MeanDelayMapper extends Mapper<Object, Text, MeanDelayCompositeKey,
    DoubleWritable> {
    int year = 0;
    Set<Integer> months = new HashSet<>();
    Set<String> airlines = new HashSet<>();
    Set<String> origins = new HashSet<>();
    Set<String> dest = new HashSet<>();
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
        Path flightListPath = new Path(context.getConfiguration().get("flightList"));
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(
            flightListPath)));
        String line;
        line = br.readLine();
        String record[] = new String[15];
        while (line != null) {
            record = line.split("\\t");
            if (year == 0) {
                year = Integer.parseInt(record[0]);
            }
            months.add(Integer.parseInt(record[1]));
            airlines.add(record[5]);
            origins.add(record[6]);
            dest.add(record[7]);
            line = br.readLine();
        }
    }
    
    public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
        double delay = 0;
        MeanDelayCompositeKey k;
        String line[] = value.toString().split(",");
        if (filterRecord(line)) {
            // line[42] corresponds to cancelled column
            if (line[42].equals("1")) {
                delay = 4.0;
            } else {
                double arrDelay;
                double elapsedTime;
                if (line[38].isEmpty()) {
                    arrDelay = 0;
                } else {
                    arrDelay = Double.parseDouble(line[38]);
                }
                elapsedTime = Double.parseDouble(line[45]);
                delay = arrDelay / elapsedTime;
            }
            k = new MeanDelayCompositeKey(Integer.parseInt(
                line[0]), Integer.parseInt(line[2]), line[13], line[22], line[6]);
            context.write(k, new DoubleWritable(delay));
        }
    }
    
    private boolean filterRecord(String record[]) {
        if ((Integer.parseInt(record[0]) < year) && (months.contains(
            Integer.parseInt(record[2]))) && (airlines.contains(record[6])) &&
            (origins.contains(record[13])) && (dest.contains(record[22]))) {
            return true;
        } else {
            return false;
        }
    }
}
