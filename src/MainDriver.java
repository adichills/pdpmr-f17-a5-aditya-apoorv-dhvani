// @Author : Apoorv Anand, Aditya Kammardi Sathyanarayan, Dhvani Sheth

import java.util.ArrayList;
import java.util.List;

/*
    This class is used as the main entry point
    Below are the various jobs that are run
    1) Compute the hops for each input in the input tuple list
    2) Compute the delay and cancelled information by month ,airline ,origin 
       and destination for all the years prior to the year in the input
    3) Filter the hops (First flight and Second Flight pair where the first 
       flight arrives late or is cancelled more than 20% of time historically
    4) Filter the hops (First flight and Second Flight pair where the second 
       flight is cancelled more than 20% of the times historically
    5) Calculate the hops again with the actual arrival time and actual 
       departure time for computing the Number of correct records
 */

public class MainDriver {
    public static void main(String []args) throws Exception {
        List<String> list = new ArrayList<String>();
        String hopsOutput = "hopsOutput";
        String delayCancelOutput = "delayCancelOutput";
        String filterFirstHopOutput = "firstHopOutput";
        list = HopDriver.drive(args[0],"crs", args[1], hopsOutput);
        DelayCancelDriver.drive(args[0], args[1], delayCancelOutput);
        list.addAll(FilterDriver.drive("FirstHop", hopsOutput, delayCancelOutput,
            filterFirstHopOutput));
        list.addAll(FilterDriver.drive("SecondHop", filterFirstHopOutput, 
            delayCancelOutput, "output"));
        list.addAll(HopDriver.drive(args[0], "actual", args[1], "output2"));
        CommonUtil.fileWrite(list, args[2]);
    }
}
