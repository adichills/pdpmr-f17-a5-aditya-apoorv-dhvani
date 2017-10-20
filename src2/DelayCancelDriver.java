import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

public class DelayCancelDriver {

    public static void drive(String flightData,String input,String output) throws IOException,
            ClassNotFoundException, InterruptedException{

        String year = getYearFromInput(input);


        Configuration conf = new Configuration();
        conf.set("inputYear",year);
        Job job = Job.getInstance(conf, "Delay and Cancel");

        job.setJarByClass(DelayCancelDriver.class);
        job.setReducerClass(DelayCancelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(DelayCancelMapper.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);

        FileInputFormat.addInputPath(job,new Path(flightData));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);

    }

    private static String getYearFromInput(String filePath) throws IOException{
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
            String record = records[0];
            String year = record.split(",")[0];
            return  year;



        } finally {
            IOUtils.closeStream(in);
        }

    }
}
