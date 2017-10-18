// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

public class MainDriver {
    public static void main(String []args) throws Exception {
        String inputAfterClean = "cleanedInput";
        String hopsOutput = "hopsOutput";
        String outActiveAirline = "active airline";
        String outTopAirline = "outputAirline";
        String outTopAirport = "outputAirport";
        String outActiveAirport = "active airport";
        //Clean the input data
        CleanDataDriver.drive(args[0],inputAfterClean);

        HopDriver.drive(inputAfterClean,hopsOutput);

    }
}

