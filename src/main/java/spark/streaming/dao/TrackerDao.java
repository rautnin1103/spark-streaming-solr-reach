package spark.streaming.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedAction;
import java.sql.*;
import java.util.Objects;
import java.util.Properties;

import static spark.streaming.util.QueryUtil.insertDestPathToEventTracker;

public class TrackerDao implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(TrackerDao.class);

    private Properties config;

    private String dbUrl;
    private String tableName;
    private boolean printQuery;

    private static final String DB_CONNECTION_URL = "jdbc.connection.url";
    private static final String REPLAY_TABLE_NAME = "hbase.replay.table";

    public TrackerDao(Properties config) {
        this.config = config;
        setDBConnectionUrl();
        setTableName();
        setPrintQueryOption();
    }

    private void setPrintQueryOption() {
        if(System.getenv("printQuery")!=null && System.getenv("printQuery").equals("true")) {
            printQuery = true;
        }
    }



    private void  setDBConnectionUrl(){
        if(config.getProperty(DB_CONNECTION_URL)!=null) {
            this.dbUrl = config.getProperty(DB_CONNECTION_URL);
        }
        else if(System.getenv(DB_CONNECTION_URL)!=null) {
            this.dbUrl  = System.getenv(DB_CONNECTION_URL);//"jdbc:phoenix:localhost:2183"
        } else {
            throw new IllegalStateException("DB Connection URL not found");
        }
    }

    private void setTableName(){
        if(config.getProperty(REPLAY_TABLE_NAME)!=null) {
            this.tableName = config.getProperty(REPLAY_TABLE_NAME);
        } else if(System.getenv(REPLAY_TABLE_NAME)!=null) {
            this.tableName = System.getenv(REPLAY_TABLE_NAME);//"SYSTEM.eventtracker"
        } else {
            throw new IllegalStateException("Replay Table not found");
        }
    }

    public void insertSolrStreamUpdate(String timeStamp, String data){
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try{
//            Properties properties = new Properties();
//            properties.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
//            properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
//            connection = DriverManager.getConnection(dbUrl, properties);
            connection = getDbConnection();
            logger.info("Connection::"+ connection);
            String query = insertDestPathToEventTracker(timeStamp,data,tableName);
            logger.info("DB_URL::"+dbUrl);
            if(printQuery) {
                logger.info("Query::"+query);
            }
            statement = connection.createStatement();
            statement.executeUpdate(query);
            connection.commit();


        } catch (SQLException e) {
            logger.error("failed to insert to hbase", e);
        } finally {
            if(statement != null) {
                try {
                    statement.close();
                }
                catch(Exception e) {}
            }
            if(rs != null) {
                try {
                    rs.close();
                }
                catch(Exception e) {}
            }
            if(connection != null) {
                try {
                    connection.close();
                }
                catch(Exception e) {}
            }
        }
    }


    private Connection getDbConnection()
    {
        try
        {
            Class.forName(config.getProperty("jdbc.driver"));
        }
        catch (ClassNotFoundException e)
        {
            logger.error("HBase database driver not found", e);
        }
        try
        {
            Configuration conf = new Configuration(false);
            conf.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
            conf.set("hadoop.security.authentication", "Kerberos");
            logger.info(config.getProperty("kerberos.principle"));
            logger.info(config.getProperty("kerberos.keytab"));
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation ugi = UserGroupInformation
                    .loginUserFromKeytabAndReturnUGI(config.getProperty("kerberos.principle"), config.getProperty("kerberos.keytab"));
            return ugi.doAs((PrivilegedAction<Connection>) () -> {
                try
                {
                    logger.info("DB_URL::"+dbUrl);
                    return DriverManager
                            .getConnection(Objects.requireNonNull(dbUrl));
                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
                return null;
            });
        }
        catch (IOException e)
        {
            logger.error("failed to created db connection", e);
        }
        return null;
    }


}
