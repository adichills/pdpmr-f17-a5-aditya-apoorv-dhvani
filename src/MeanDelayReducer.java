// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// the reducer has all the normalized delay values per year, month, airport and
// airline. Calculate the mean and output it.
public class MeanDelayReducer extends Reducer<MeanDelayCompositeKey, DoubleWritable,
    Text, DoubleWritable> {
    public void reduce(MeanDelayCompositeKey key, Iterable<DoubleWritable> values, 
        Context context) throws IOException, InterruptedException {
        DoubleWritable mean = new DoubleWritable();
        double sum  = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        mean.set(sum/count);
        context.write(new Text(key.getYear() + "," + key.getMonth() + "," + 
            key.getOrigin() + "," + key.getDestination() + "," + key.getAirline() 
            + ","), mean);
    }
}
