package spark.streaming.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryUtil {

    public static String insertDestPathToEventTracker(String timeStamp, String destinationPath, String tableName) {
        String query=null;
        query = "upsert into "+ tableName+" values (" +
                "'"+timeStamp+"'"+"," +
                "'metadata'," +
                "'"+destinationPath+"'"+")";
        return query;
    }

    private static String getCurrentDate(){
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }

    public static String creatSingleFileSearchQuery(String fileName, String tableName){

        String query=null;
        query = "select ID,replay from "+ tableName+ " where ID='"+fileName+"'";
        return query;
    }

    public static String createDateRangeSearchQuery(String fromDate, String toDate, String tableName){
        String query=null;

        query = "SELECT ID, replay \n" +
                "FROM "+tableName+" \n" +
                "WHERE Created_DATE > to_timestamp('"+fromDate+"') \n" +
                "AND Created_DATE < to_timestamp('"+toDate+"') \n" +
                "AND ((indexing='0' OR indexing='')\n" +
                     "OR (persistence='0' OR persistence=''))";
        return query;
    }





}
