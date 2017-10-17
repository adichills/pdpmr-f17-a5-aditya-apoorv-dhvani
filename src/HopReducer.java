import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HopReducer extends Reducer<Text,Text,Text,Text> {
    private Text joinedResult;

    @Override
    protected void setup(Context context){

    }


    public void reduce(Text text,Iterable<Text> values,Context context){


    }
}
