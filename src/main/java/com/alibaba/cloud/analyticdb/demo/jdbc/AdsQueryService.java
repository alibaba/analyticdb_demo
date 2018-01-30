package com.alibaba.cloud.analyticdb.demo.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.druid.pool.DruidDataSource;

public class AdsQueryService {
    private static Log log = LogFactory.getLog(AdsQueryService.class);

    /**
     * Druid connection pool configuration parameters.
     */
    public static final int DRUID_DS_PARM_MAX_ACTIVE = 50;
    public static final int DRUID_DS_PARM_INITIAL_SIZE = 5;
    public static final int DRUID_DS_PARM_MIN_IDLE = 10;
    public static final int DRUID_DS_PARM_MAX_WAIT = 60000;
    public static final int DRUID_DS_PARM_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 2000;
    public static final int DRUID_DS_PARM_MIN_EVICTABLE_IDLE_TIME_MILLIS = 600000;
    public static final int DRUID_DS_PARM_MAX_EVICTABLE_IDLE_TIME_MILLIS = 900000;
    public static final String DRUID_DS_PARM_VALIDATION_QUERY = "show status like '%Service_Status%'";
    public static final boolean DRUID_DS_PARM_TEST_WHILE_IDLE = true;
    public static final boolean DRUID_DS_PARM_TEST_ON_BORROW = false;
    public static final boolean DRUID_DS_PARM_TEST_ON_RETURN = false;
    public static final boolean DRUID_DS_PARM_REMOVE_ABANDONED = true;
    public static final int DRUID_DS_PARM_REMOVE_ABANDONED_TIMEOUT = 180;

    private Map<String, DruidDataSource> dataSources = null;

    /**
     * Construct and register ADS database data sources.
     * 
     * @param jdbcDataSources
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public AdsQueryService(Map<String, List<String>> jdbcDataSources) throws ClassNotFoundException, SQLException {
        dataSources = new HashMap<String, DruidDataSource>();
        for (Entry<String, List<String>> jdbcDataSource : jdbcDataSources.entrySet()) {
            String database = jdbcDataSource.getKey();
            String jdbcURL = jdbcDataSource.getValue().get(0);
            dataSources.put(database, createDruidDataSource(jdbcDataSource.getValue()));
            log.info("ADS_DATA_SOURCE_REGISTER database=" + database + " URL=" + jdbcURL);
        }
    }

    /**
     * Create druid data source from the JDBC URL parameters: Parameter 1: JDBC URL, such as
     * "jdbc:mysql://127.0.0.1:10009/test_db?characterEncoding=UTF-8" Parameter 2: user name Parameter 3: password
     * 
     * @param jdbcURLParams
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private DruidDataSource createDruidDataSource(List<String> jdbcURLParams)
            throws ClassNotFoundException, SQLException {
        String driverClassName = "com.mysql.jdbc.Driver";
        Class.forName(driverClassName);

        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(jdbcURLParams.get(0));
        ds.setUsername(jdbcURLParams.get(1));
        ds.setPassword(jdbcURLParams.get(2));
        ds.setDriverClassName(driverClassName);
        ds.setInitialSize(DRUID_DS_PARM_INITIAL_SIZE);
        ds.setMinIdle(DRUID_DS_PARM_MIN_IDLE);
        ds.setMaxActive(DRUID_DS_PARM_MAX_ACTIVE);
        ds.setMaxWait(DRUID_DS_PARM_MAX_WAIT);
        ds.setTimeBetweenEvictionRunsMillis(DRUID_DS_PARM_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        ds.setMinEvictableIdleTimeMillis(DRUID_DS_PARM_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        ds.setMaxEvictableIdleTimeMillis(DRUID_DS_PARM_MAX_EVICTABLE_IDLE_TIME_MILLIS);
        ds.setValidationQuery(DRUID_DS_PARM_VALIDATION_QUERY);
        ds.setTestWhileIdle(DRUID_DS_PARM_TEST_WHILE_IDLE);
        ds.setTestOnBorrow(DRUID_DS_PARM_TEST_ON_BORROW);
        ds.setTestOnReturn(DRUID_DS_PARM_TEST_ON_RETURN);
        ds.setRemoveAbandoned(DRUID_DS_PARM_REMOVE_ABANDONED);
        ds.setRemoveAbandonedTimeout(DRUID_DS_PARM_REMOVE_ABANDONED_TIMEOUT);

        Connection initialDummyConn = ds.getConnection();
        initialDummyConn.close();

        return ds;
    }

    /**
     * Execute query.
     * 
     * @param query
     */
    public void executeQuery(String query) {
        boolean executeSuccess = false;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        // Try to execute query on registered data sources in the registering order.
        for (Entry<String, DruidDataSource> dsEntry : dataSources.entrySet()) {
            String database = dsEntry.getKey();
            DruidDataSource ds = dsEntry.getValue();
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery(query);

                // Handle the result set.
                // TO-DO: user application logic here ...

                executeSuccess = true;
            } catch (Throwable t) {
                log.error("QUERY_EXEC_FAILED_SINGLE_DATA_SOURCE database=" + database, t);

            } finally {
                closeResource(conn, stmt, rs);
            }

            // If query execute successfully, then no need to try other data source. If failed, then continue with the
            // next data source.
            if (executeSuccess) {
                break;
            }
        }

        // If query failed on all registered data sources, then the query failed totally.
        if (!executeSuccess) {
            log.error("QUERY_EXEC_FAILED_ALL_DATA_SOURCE");
        }
    }

    /**
     * Close connection resource.
     */
    public static void closeResource(Connection conn, Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.error(e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.error(e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error(e);
            }
        }
    }

    // For test.
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Map<String, List<String>> jdbcDataSources = new HashMap<String, List<String>>();
        jdbcDataSources.put("ads_db_1", Arrays.asList(
                "jdbc:mysql://ads_db_1-xxxx-cn-hz.xxx.com:10001/ads_db_1?characterEncoding=UTF-8", "user", "password"));
        jdbcDataSources.put("ads_db_2", Arrays.asList(
                "jdbc:mysql://ads_db_2-xxxx-cn-hz.xxx.com:10002/ads_db_2?characterEncoding=UTF-8", "user", "password"));

        AdsQueryService adsQueryService = new AdsQueryService(jdbcDataSources);
        adsQueryService.executeQuery("SELECT ...");
    }
}
