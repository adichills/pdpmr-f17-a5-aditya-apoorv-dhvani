// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import java.util.ArrayList;
import java.util.List;

public class MainDriver {
    public static void main(String []args) throws Exception {
        List<String> list = new ArrayList<String>();
        String hopsOutput = "hopsOutput";
        String delayCancelOutput = "delayCancelOutput";
        String filterFirstHopOutput = "firstHopOutput";
        list = HopDriver.drive(args[0],"crs",args[1],hopsOutput);
        DelayCancelDriver.drive(args[0],args[1],delayCancelOutput);
        list.addAll(FilterDriver.drive("FirstHop",hopsOutput,delayCancelOutput,filterFirstHopOutput));
        list.addAll(FilterDriver.drive("SecondHop",filterFirstHopOutput,delayCancelOutput,"output"));
        list.addAll(HopDriver.drive(args[0],"actual",args[1],"output2"));
        CommonUtil.fileWrite(list, args[2]);
    }
}

