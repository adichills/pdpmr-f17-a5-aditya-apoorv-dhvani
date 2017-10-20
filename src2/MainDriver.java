// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

public class MainDriver {
    public static void main(String []args) throws Exception {
        String inputAfterClean = "cleanedInput";
        String hopsOutput = "hopsOutput";
        String delayCancelOutput = "delayCancelOutput";
        String outTopAirline = "outputAirline";
        String outTopAirport = "outputAirport";
        String outActiveAirport = "active airport";

        String filterFirstHopOutput = "firstHopOutput";

        HopDriver.drive(args[0],"crs",args[1],hopsOutput);
        DelayCancelDriver.drive(args[0],args[1],delayCancelOutput);


        FilterDriver.drive("FirstHop",hopsOutput,delayCancelOutput,filterFirstHopOutput);
        FilterDriver.drive("SecondHop",filterFirstHopOutput,delayCancelOutput,"output");

        HopDriver.drive(args[0],"actual",args[1],"output2");

    }
}

