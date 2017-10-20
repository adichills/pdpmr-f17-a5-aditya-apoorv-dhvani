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
        //Clean the input data
        CleanDataDriver.drive(args[0],inputAfterClean);

        HopDriver.drive(inputAfterClean,args[1],hopsOutput);
        DelayCancelDriver.drive(inputAfterClean,args[1],delayCancelOutput);


        FilterDriver.drive("FirstHop",hopsOutput,delayCancelOutput,filterFirstHopOutput);
        FilterDriver.drive("SecondHop",filterFirstHopOutput,delayCancelOutput,"output");

    }
}

