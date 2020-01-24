package spark.streaming.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.DaemonStream;
import org.apache.solr.client.solrj.io.stream.TopicStream;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.StreamTopic2;
import spark.streaming.dao.TrackerDao;

import java.io.IOException;
import java.util.*;

public class SolrStreamingUtil {
    private static final Logger logger = LoggerFactory.getLogger(StreamTopic2.class);

    public static DaemonStream createDaemonStream(String zooKeeperHost , String topicQuery, String docFields,
                                                   String chkPointCollection, String collection, String topicId) {
        Map<String, String[]> topicQueryParams = new HashMap<String, String[]>();
        topicQueryParams.put("q",new String[]{topicQuery});    // The query for the topic
        topicQueryParams.put("rows", new String[]{"500"});// How many rows to fetch during each run
        topicQueryParams.put("fl", new String[]{docFields});  // The field list to return with the documents
        SolrParams solrPararms =  new MultiMapSolrParams(topicQueryParams);
        TopicStream topicStream = new TopicStream(zooKeeperHost,         // Host address for the zookeeper service housing the collections
                chkPointCollection,   // The collection to store the topic checkpoints
                collection,// The collection to query for the topic records
                topicId,       // The id of the topic
                1,
                -1,// checkpoint every X tuples, if set -1 it will checkpoint after each run.
                solrPararms);
        DaemonStream daemonStream = new DaemonStream(topicStream, // The underlying stream to run.
                "prueba2",    // The id of the daemon
                10000,        // The interval at which to run the internal stream
                500);
        return daemonStream;
    }

    public static Dataset buildSparkContext(SparkSession spark, String zooKeeperHost, String collection) {
        // TODO Auto-generated method stub

        System.out.println("Running ver 3 after simplifying the Tracker- " );
        String queryStr = "*:*";
        Map<String, String> options = new HashMap<String, String>();
        options.put("zkhost", zooKeeperHost);
        options.put("collection", collection);
        options.put("query", queryStr);
        options.put("fields","file,message,timeStamp");
        options.put("request_handler", "/select"); //"/export"

        Dataset dsSolr = spark.read().format("solr").options(options).load();
        dsSolr.printSchema();

        dsSolr.show();
        return dsSolr;
    }

    public static void workWithTuples(DaemonStream daemonStream, SparkSession spark, Dataset dsSolr, TrackerDao trackerDao) {
        while(true) {
            Tuple tuple;
            try {
                tuple = daemonStream.read();
                if(tuple.EOF) {
                    break;
                } else {
                    System.out.println(tuple.fields.toString());

                    // convert map to JSON String
                    try {
                        ObjectMapper mapperObj = new ObjectMapper();

                        String jsonResp = mapperObj.writeValueAsString(tuple.fields);
                        logger.info(jsonResp);
                        Object timeStamp = tuple.fields.get("timeStamp");
                        logger.info(String.valueOf(timeStamp));
                        String timeStampStr = (String) tuple.fields.get("timeStamp");
                        System.out.println(jsonResp);
                        trackerDao.insertSolrStreamUpdate((String) timeStampStr, jsonResp);
                        List<String> jsonData = Arrays.asList(
                                jsonResp);
                        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
                        Dataset freshSolrDataset = spark.read().json(anotherPeopleDataset);
                        dsSolr.show();
                        freshSolrDataset.show();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
