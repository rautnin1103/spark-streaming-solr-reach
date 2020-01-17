package spark.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.stream.DaemonStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.dao.TrackerDao;

import static spark.streaming.solr.SolrStreamingUtil.*;
import static spark.streaming.util.CmdUtils.getSolrStreamingCmdLineOptions;

public class StreamTopic2
{
	private static final Logger logger = LoggerFactory.getLogger(StreamTopic2.class);

	private static String zkHostString;
	private static String jaasConfig;
	private static String topicQuery;
	private static String docFields;
	private static String chkPointCollection;
	private static String collection;
	private static String topicId;
	private static String hbaseTableName;

	public static void main( String[] args ) 
	{
		if(jaasConfig != null && !jaasConfig.isEmpty()) {
			logger.info("Setting Up Jaas Config");
			System.setProperty("java.security.auth.login.config", jaasConfig);
		}
		Options options = getSolrStreamingCmdLineOptions();
		CommandLineParser parser = new BasicParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			logger.error(e.getMessage());
			formatter.printHelp("SparkStreaming", options);
			System.exit(1);
			return;
		}
		setParams(cmd);
		StreamContext context = new StreamContext();
		SolrClientCache cache = new SolrClientCache();
		context.setSolrClientCache(cache);
		DaemonStream daemonStream = createDaemonStream(zkHostString, topicQuery, docFields, chkPointCollection, collection, topicId);
		daemonStream.setStreamContext(context);
		daemonStream.open();
		SparkSession spark = SparkSession
				.builder()
				.master("local[2]")
				.appName("JavaReachStreamSolr")
				.config("spark.cleaner.ttl","2000")
				.getOrCreate();
		Dataset dsSolr = buildSparkContext(spark, zkHostString, collection);
		try (InputStream input = StreamTopic2.class.getClassLoader().getResourceAsStream("application.properties")) {
			Properties prop = new Properties();

			if (input == null) {
				System.out.println("Sorry, unable to find config.properties");
				return;
			}
			//load a properties file from class path, inside static method
			prop.load(input);
			if(hbaseTableName!=null && !hbaseTableName.isEmpty()) {
				prop.setProperty("hbase.replay.table",hbaseTableName);
			}
			logger.info(prop.getProperty("jdbc.connection.url"));

			TrackerDao trackerDao = new TrackerDao(prop);

			workWithTuples(daemonStream,spark,dsSolr,trackerDao); //here we work with the Tuples

		} catch (IOException ex) {
			logger.error("Failed to run replay", ex);
		}
		daemonStream.close();
	}

	private static void setParams(CommandLine cmd) {
		zkHostString = cmd.getOptionValue("zkHostString");
		jaasConfig = cmd.getOptionValue("jaasConfig");
		topicQuery= cmd.getOptionValue("topicQuery");
		docFields= cmd.getOptionValue("docFields");
		chkPointCollection= cmd.getOptionValue("chkPointCollection");;
		collection= cmd.getOptionValue("collection");;
		topicId= cmd.getOptionValue("topicId");
		hbaseTableName = cmd.getOptionValue("hbaseTableName");
	}
}