// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// calculate the normalized delay and output that as value and key as defined in
// MeanDelayCompositeKey. 
public class MeanDelayMapper extends Mapper<Object, Text, MeanDelayCompositeKey,
    Text> {
    Set<MeanDelayCompositeKey> firstHopFilghts = new HashSet<>();
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
        Path flightListPath = new Path(context.getConfiguration().get("flightList"));
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(
            flightListPath)));
        String line;
        line = br.readLine();
        String record[] = new String[3];
        String firstHop[] = new String[8];
        while (line != null) {
            record = line.split("\\s+");
            firstHop = record[1].split(",");
            firstHopFilghts.add(new MeanDelayCompositeKey(
                Integer.parseInt(firstHop[0]), Integer.parseInt(firstHop[1]), 
                firstHop[6], firstHop[7], firstHop[5]));
            line = br.readLine();
        }
    }
    
    public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
        Text delayOrCancelled = new Text();
        MeanDelayCompositeKey k;
        String line[] = value.toString().split(",");
        k = new MeanDelayCompositeKey(Integer.parseInt(
            line[0]), Integer.parseInt(line[2]), line[13], line[22], line[6]);
        if (firstHopFilghts.contains(k)) {
            // line[42] corresponds to cancelled column
            if (line[42].equals("1")) {
                delayOrCancelled.set("*1");
            } else {
                if (line[38].isEmpty()) {
                    delayOrCancelled.set(new Text("0"));
                } else {
                    delayOrCancelled.set(new Text(line[38]));
                }
            }
        }
        context.write(k, delayOrCancelled);
    }
}
