package spark.streaming.util;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CmdUtils {
    public static Options getSolrStreamingCmdLineOptions() {
        Options options = new Options();

        Option zkHostString = new Option("zkh", "zkHostString", true, "zookeeper host");
        zkHostString.setRequired(true);
        options.addOption(zkHostString);

        Option jassConfig = new Option("auth", "jassConfig", true, "jass config");
        jassConfig.setRequired(false);
        options.addOption(jassConfig);

        Option queryString = new Option("tq", "topicQuery", true, "solr query string for topic");
        queryString.setRequired(true);
        options.addOption(queryString);

        Option docFields = new Option("fl", "docFields", true, "document fields to return");
        docFields.setRequired(true);
        options.addOption(docFields);

        Option checkPointCollection = new Option("cpcl", "chkPointCollection", true, "check point collection");
        checkPointCollection.setRequired(true);
        options.addOption(checkPointCollection);

        Option collection = new Option("cl", "collection", true, "target collection");
        collection.setRequired(true);
        options.addOption(collection);

        Option topicId = new Option("tp", "topicId", true, "topic id");
        collection.setRequired(true);
        options.addOption(topicId);

        Option hbaseTableName = new Option("tbl", "hbaseTableName", true, "hbase table name");
        collection.setRequired(true);
        options.addOption(hbaseTableName);

        return options;

    }
}
