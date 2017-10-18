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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class SecondHopMapper extends Mapper<Object, Text, Text, Text> {
    Set<String> yearList = new HashSet<String>();
    Set<String> dayList = new HashSet<String>();
    Set<String> monthList = new HashSet<String>();
    Set<String> destiantionList = new HashSet<String>();
    private static final String CSV_SEP = ",";
    HashMap<String,InputField> validRecordFields = new HashMap<String,InputField>();
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

        StringBuilder sb = new StringBuilder();
        // year
        sb.append(record[0]);
        //month
        sb.append(record[1]);
        //day
        sb.append(record[2]);
        //destination
        sb.append(record[22]);

        Integer day = Integer.parseInt(record[2]);

        String key = sb.toString();
        if (validRecordFields.containsKey(key)){
            if(record[13].equals(validRecordFields.get(key).origin)){
                return false;
            }
            else{
                return  true;
            }
        }

        //TODO handle next day flights as well

        return false;
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
                InputField inputField = new InputField();
                inputField.year = fields[0];
                inputField.month = fields[1];
                inputField.day = fields[2];
                inputField.origin = fields[3];
                inputField.destination = fields[4];
                sb.append(inputField.year);
                sb.append(inputField.month);
                sb.append(inputField.day);
                sb.append(inputField.destination);
                validRecordFields.put(sb.toString(),inputField);
            }



        } finally {
            IOUtils.closeStream(in);
        }

    }
}
