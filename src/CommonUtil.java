// @Author : Apoorv Anand, Aditya Kammardi Sathyanarayan, Dhvani Sheth

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

// this class has methods which are used multiple times in the program. for eg.
// checking whether the record is proper or not.

public class CommonUtil {

    private static String COMMA_SEPARATOR = ",";

    protected static boolean cleanData(CSVRecord record) {
        return isRecordValid(record) && (record.fieldCount == 110);
    }

    private static boolean isRecordValid(CSVRecord record) {
        return isCRSTimeValid(record) && isTimeZoneValid(record)
            && isAirportIdValid(record) && isAirportSeqIdValid(record)
            && isCityMarketIdValid(record) && isStateFipsValid(record)
            && isWacValid(record) && isOriginValid(record)
            && isDestValid(record) && isCityValid(record)
            && isCityValid(record) && isStateValid(record)
            && isCancelledValid(record)
            && isNotCancelledTimeZoneValid(record)
            && isArrDelayValid(record) && isArrDelayMinsValid(record);
    }

    /*
        29 - CRS_DEP_TIME
        40 - CRS_ARR_TIME
    */
    private static boolean isCRSTimeValid(CSVRecord record) {
        if (null != record.get(29) && record.get(40) != null) {
            try {
                return record.get(29).length() > 0
                    && Integer.parseInt(record.get(29)) != 0
                    && record.get(40).length() > 0
                    && Integer.parseInt(record.get(40)) != 0;
            } catch (NumberFormatException e) {
                return false;
            }
        } else
            return false;
    }

    /*
        40 - CRS_ARR_TIME
        29 - CRS_DEP_TIME
        50 - CRS_ELAPSED_TIME
    */
    private static int getTimezone(CSVRecord record) {
        try {
            int totalArrMinutes = convertIntoMinutes(record.get(40));
            int totalDepMinutes = convertIntoMinutes(record.get(29));
            if (!(totalArrMinutes == -1 || totalDepMinutes == -1)) {
                int timezone = totalArrMinutes - totalDepMinutes
                    - Integer.parseInt(record.get(50));
                return timezone;
            }
            return -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    protected static int convertIntoMinutes(String time) {
        try {
            if (time.length() < 3 || time.length() > 4)
                return -1;
            else if (time.length() == 3) {
                int hours = Integer.parseInt("" + time.charAt(0));
                int minutes = Integer.parseInt("" + time.charAt(1) + time.charAt(2));
                return hours * 60 + minutes;
            } else {
                int hours = Integer.parseInt("" + time.charAt(0) + time.charAt(1));
                int minutes = Integer.parseInt("" + time.charAt(2) + time.charAt(3));
                return hours * 60 + minutes;
            }
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static boolean isTimeZoneValid(CSVRecord record) {
        return (getTimezone(record) % 60 == 0);
    }

    /*
        11 - ORIGIN_AIRPORT_ID
        20 - DEST_AIRPORT_ID
    */
    private static boolean isAirportIdValid(CSVRecord record) {
        try {
            return Integer.parseInt(record.get(11)) > 0
                && Integer.parseInt(record.get(20)) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
        12 - ORIGIN_AIRPORT_SEQ_ID
        21 - DEST_AIRPORT_SEQ_ID

    */
    private static boolean isAirportSeqIdValid(CSVRecord record) {
        try {
            return Integer.parseInt(record.get(12)) > 0
                && Integer.parseInt(record.get(21)) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
        13 - ORIGIN_CITY_MARKET_ID
        22 - DEST_CITY_MARKET_ID
    */
    private static boolean isCityMarketIdValid(CSVRecord record) {
        try {
            return Integer.parseInt(record.get(13)) > 0
                && Integer.parseInt(record.get(22)) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
        17 - ORIGIN_STATE_FIPS
        26 - DEST_STATE_FIPS
    */
    private static boolean isStateFipsValid(CSVRecord record) {
        try {
            return Integer.parseInt(record.get(17)) > 0
                && Integer.parseInt(record.get(26)) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
        19 - ORIGIN_WAC
        28 - DEST_WAC
    */
    private static boolean isWacValid(CSVRecord record) {
        try {
            return Integer.parseInt(record.get(19)) > 0
                && Integer.parseInt(record.get(28)) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
        14 - ORIGIN
    */
    private static boolean isOriginValid(CSVRecord record) {
        return record.get(14).length() > 0;
    }

    /*
        23 - DEST
    */
    private static boolean isDestValid(CSVRecord record) {
        return record.get(23).length() > 0;
    }

    /*
        15 - ORIGIN_CITY_NAME
    */
    private static boolean isCityValid(CSVRecord record) {
        return record.get(15).length() > 0 && record.get(24).length() > 0;
    }

    /*
        16 - ORIGIN_STATE_ABR
        18 - ORIGIN_STATE_NM
        25 - DEST_STATE_ABR
        27 - DEST_STATE_NM
    */
    private static boolean isStateValid(CSVRecord record) {
        return record.get(16).length() > 0 && record.get(18).length() > 0
            && record.get(25).length() > 0 && record.get(27).length() > 0;
    }

    /*
        47 - CANCELLED
    */
    private static boolean isCancelledValid(CSVRecord record) {
        try {
            return Integer.parseInt(record.get(47)) == 0
                || Integer.parseInt(record.get(47)) == 1;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
        41 - ARR_TIME
        30 - DEP_TIME
        51 - CRS_ELAPSED_TIME
    */
    private static boolean isNotCancelledTimeZoneValid(CSVRecord record) {
        if (!isFlightCancelled(record)) {
            int arr = record.get(41).length() > 0 ? convertIntoMinutes(record
                .get(41)) : 0;
            int dep = record.get(30).length() > 0 ? convertIntoMinutes(record
                .get(30)) : 0;
            int aet = record.get(51).length() > 0 ? Integer
                .parseInt(record.get(51)) : 0;
            return (arr - dep - aet - getTimezone(record)) == 0;
        }
        return true;
    }

    /*
        47 - CANCELLED
    */
    private static boolean isFlightCancelled(CSVRecord record) {
        try {
            return Integer.parseInt(record.get(47)) == 0
                || record.get(47).length() == 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
        42 - ARR_DELAY
        43 - ARR_DELAY_NEW
    */
    private static boolean isArrDelayValid(CSVRecord record) {
        try {
            double arrDelay = Double.parseDouble(record.get(42));
            if (arrDelay > 0) {
                return arrDelay == Double.parseDouble(record.get(43));
            } else if (arrDelay < 0) {
                return Double.parseDouble(record.get(43)) == 0.0;
            } else
                return false;
            } catch (NumberFormatException e) {
                return false;
        }
    }

    /*
        43 - ARR_DELAY_NEW
        44 - ARR_DEL15
    */
    private static boolean isArrDelayMinsValid(CSVRecord record) {
        try {
            if (Double.parseDouble(record.get(43)) >= 15)
                return Double.parseDouble(record.get(44)) == 1;
            else
                return true;
            } catch (NumberFormatException e) {
                return false;
        }
    }

    protected static void fileWrite(List<String> list, String fileName){
        FileWriter writer = null;
        try {
            writer = new FileWriter(fileName);
            for (String s : list){
                writer.append(s);
                writer.append(COMMA_SEPARATOR);
            }
        }
        catch (IOException e){
            System.out.println(e.toString());
        }
        finally {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                System.out.println(e.toString());
            }
        }
    }
}
