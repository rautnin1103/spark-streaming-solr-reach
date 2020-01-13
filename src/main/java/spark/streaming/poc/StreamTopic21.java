package spark.streaming.poc;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.DaemonStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TopicStream;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StreamTopic21 
{
	public static void main( String[] args ) 
	{
		String zookeeperHost = "localhost:9983";
		StreamContext context = new StreamContext();
		SolrClientCache cache = new SolrClientCache();
		context.setSolrClientCache(cache);

		Map<String, String[]> topicQueryParams = new HashMap<String, String[]>(); 
		topicQueryParams.put("q",new String[]{"*"});    // The query for the topic
		topicQueryParams.put("rows", new String[]{"500"});// How many rows to fetch during each run
		topicQueryParams.put("fl", new String[]{"*"});    // The field list to return with the documents

		SolrParams solrPararms =  new MultiMapSolrParams(topicQueryParams);

		TopicStream topicStream = new TopicStream(zookeeperHost,         // Host address for the zookeeper service housing the collections 
				"chk1",   // The collection to store the topic checkpoints
				"test1",// The collection to query for the topic records
				"topicId1",       // The id of the topic
				1,      
				-1,// checkpoint every X tuples, if set -1 it will checkpoint after each run.
				solrPararms);    // The query parameters for the TopicStream

		DaemonStream daemonStream = new DaemonStream(topicStream, // The underlying stream to run. 
				"prueba2",    // The id of the daemon
				1000,        // The interval at which to run the internal stream
				500);        // The internal queue size for the daemon stream. Tuples will be placed in the queue
		// as they are read by the internal internal thread.
		// Calling read() on the daemon stream reads records from the internal queue.

		daemonStream.setStreamContext(context);
		daemonStream.open();


		SparkSession spark = SparkSession
				.builder()
				.master("local[2]")
				.appName("JavaReachStreamSolr")
				.config("spark.cleaner.ttl","2000")
				.getOrCreate();


		Dataset dsSolr = buildSparkContext(spark);


		workWithTuples(daemonStream,spark,dsSolr); //here we work with the Tuples

		daemonStream.close();
	}

	private static Dataset buildSparkContext(SparkSession spark) {
		// TODO Auto-generated method stub

		System.out.println("Running ver 3 after simplifying the Tracker- " );




		String queryStr = "*:*";


		Map<String, String> options = new HashMap<String, String>();
		options.put("zkhost", "localhost:9983");
		options.put("collection", "test1");
		options.put("query", queryStr);

		options.put("request_handler", "/select"); //"/export"

		Dataset dsSolr = spark.read().format("solr").options(options).load();





		dsSolr.printSchema();

		dsSolr.show();


		return dsSolr;




	}

	private static void workWithTuples(DaemonStream daemonStream,SparkSession spark, Dataset dsSolr) {
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
						System.out.println(jsonResp);
						
						List<String> jsonData = Arrays.asList(
								jsonResp);
						
						Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
						Dataset freshSolrDataset = spark.read().json(anotherPeopleDataset);


						dsSolr.show();
						freshSolrDataset.show();
						
//						Dataset mergedDS = dsSolr.union(freshSolrDataset);
//						
//						mergedDS.show();

						
						
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