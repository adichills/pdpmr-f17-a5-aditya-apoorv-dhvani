import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class SecondHopMapper extends Mapper<Object, Text, Text, Text> {
    Set<String> yearList = new HashSet<String>();
    Set<String> dayList = new HashSet<String>();
    Set<String> monthList = new HashSet<String>();
    Set<String> destiantionList = new HashSet<String>();
    private static final String CSV_SEP = ",";

    Configuration conf;
    Text outPutKey = new Text();
    Text outPutValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        conf = context.getConfiguration();

        String inputfilePath = conf.get("inputFile");

        populateInputLists(inputfilePath);
    }

    public void map(Object key, Text value, Context context) throws IOException,
    InterruptedException {
        String[] record = value.toString().split(CSV_SEP);
        if(isRecordImportant(record)){
            StringBuilder sb = new StringBuilder();
            sb.append("SecondHop");
            sb.append(CSV_SEP);
            sb.append(record[0]);
            sb.append(CSV_SEP);
            sb.append(record[1]);
            sb.append(CSV_SEP);
            sb.append(record[2]);
            sb.append(CSV_SEP);
            //Dep_Time
            sb.append(record[29]);
            sb.append(CSV_SEP);
            //Arr_Time
//            sb.append(record[35]);
//            sb.append(CSV_SEP);
            // Airline Carrier
            sb.append(record[6]);
            sb.append(CSV_SEP);
            // Origin
            sb.append(record[13]);
            sb.append(CSV_SEP);
            //Destination
            sb.append(record[22]);
            sb.append(CSV_SEP);


            outPutKey.set(record[13]);
            outPutValue.set(sb.toString());
            context.write(outPutKey,outPutValue);
        }

    }

    private boolean isRecordImportant(String[] record){
        //check if year is present in the input file
        if(!yearList.contains(record[0])){
            return false;
        }
        //check if month is present in the input file
        if(!monthList.contains(record[1])){
            return false;
        }
        //check if the day is present in the input file
        if(!dayList.contains(record[2])){
            return false;
        }
        //check if the origin is present in the input file
        if(!destiantionList.contains(record[22])){
            return false;
        }

        return true;
    }

    private void populateInputLists(String filePath) throws IOException{
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
            String [] records = fileContents.split("\n");
            for(int i = 0;i< records.length;i++){
                String [] fields = records[i].split(CSV_SEP);
                yearList.add(fields[0]);
                monthList.add(fields[1]);
                dayList.add(fields[2]);
                destiantionList.add(fields[4]);
            }



        } finally {
            IOUtils.closeStream(in);
        }

    }
}
