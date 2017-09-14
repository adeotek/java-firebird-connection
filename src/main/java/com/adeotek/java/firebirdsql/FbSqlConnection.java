package com.adeotek.java.firebirdsql;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FbSqlConnection {
    protected static final String FB_CONN_STR_PREFIX = "jdbc:firebirdsql:";
    protected static final HashMap<String, String> FB_CONN_PARAMETERS = GetDefaultConnectionParameters();
    protected static HashMap<String, String> GetDefaultConnectionParameters() {
        HashMap<String, String> params = new HashMap<>();
        params.put("sqlDialect", "3");
        params.put("charSet", "UTF-8");
//        params.put("encoding", "UTF8");
        params.put("defaultIsolation", "TRANSACTION_READ_COMMITTED");
        return params;
    }//CreateDefaultConnectionParameters

    protected String _driverName = "org.firebirdsql.jdbc.FBDriver";
    protected String _hostname = "localhost";
    protected int _port = 3050;
    protected String _username = "SYSDBA";
    protected String _password = "masterkey";
    protected String _database;
    protected String _connectionString = null;
    protected Driver _driver = null;
    protected Connection _connection = null;
    protected Logger appLogger;

    // Values: "tcp"/null (default); "local"; "native"; "embedded"
    public String connectionType = null;
    public int timeout = 3;

    public FbSqlConnection(String database, String hostname, int port, String username, String password, String driverName) {
        _database = database;
        if (!isStringEmptyOrNull(hostname)) {
            _hostname = hostname;
        }
        if (port>0) {
            _port = port;
        }
        if (!isStringEmptyOrNull(username)) {
            _username = username;
        }
        if (password!=null) {
            _password = password;
        }
        if (!isStringEmptyOrNull(driverName)) {
            _driverName = driverName;
        }
    }//FBConnection

    public FbSqlConnection(String database, String hostname, String username, String password) {
        this(database, hostname, 0, username, password, null);
    }//FBConnection

    public FbSqlConnection(String connectionString, String username, String password) {
        _connectionString = connectionString;
        if (!isStringEmptyOrNull(username)) {
            _username = username;
        }
        if (password!=null) {
            _password = password;
        }
    }//FBConnection

    public boolean SetLogger(Logger logger, Formatter formatter) {
        try {
            if (logger!=null) {
                appLogger = logger;
            } else {
                appLogger = Logger.getLogger(FbSqlConnection.class.getName());
                logger.setLevel(Level.WARNING);
                if (formatter!=null) {
                    logger.setUseParentHandlers(false);
                    ConsoleHandler handler = new ConsoleHandler();
                    handler.setFormatter(formatter);
                    logger.addHandler(handler);
                }
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }//SetLogger

    protected String prepareConnectionString() {
        if (!isStringEmptyOrNull(_connectionString)) {
            return _connectionString;
        }
        StringBuilder connStr = new StringBuilder();
        connStr.append(FB_CONN_STR_PREFIX);
        if (connectionType.trim().toLowerCase().equals("local")) {
            connStr.append("local").append(":");
        } else if (connectionType.trim().toLowerCase().equals("native")) {
            connStr.append("native").append(":");
        } else if (connectionType.trim().toLowerCase().equals("embedded")) {
            connStr.append("embedded").append(":");
        }
        connStr.append("//").append(_hostname).append(":").append(_port);
        connStr.append("/").append(_database);
        if (FB_CONN_PARAMETERS!=null && FB_CONN_PARAMETERS.size()>0) {
            boolean first = true;
            for (Map.Entry<String, String> kv : FB_CONN_PARAMETERS.entrySet()) {
                if(first) {
                    first = false;
                    connStr.append("?");
                } else {
                    connStr.append("&");
                }
                connStr.append(kv.getKey()).append("=").append(kv.getValue());
            }
        }
        return connStr.toString();
    }//prepareConnectionString

    protected void loadDriver(String driverName) throws FbSqlException {
        if (isStringEmptyOrNull(driverName)) {
            throw new FbSqlException("Invalid driver name (null or empty)");
        }
        try {
            Class.forName(driverName);
            // We pass the entire database URL, but we could just pass "jdbc:interbase:"
            _driver = DriverManager.getDriver(prepareConnectionString());
            System.out.println ("Firebird JCA-JDBC driver version " +
                    _driver.getMajorVersion () +
                    "." +
                    _driver.getMinorVersion () +
                    " registered with driver manager.");
        } catch (ClassNotFoundException cnfe) {
            throw new FbSqlException(cnfe);
        } catch (SQLException se) {
            throw new FbSqlException(se);
        }
    }//loadDriver

    public void open(boolean disableAutoCommit, HashMap<String, String> properties) throws FbSqlException {
        loadDriver(_driverName);
        try {
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", _username);
            connectionProperties.put("password", _password);
            if (FB_CONN_PARAMETERS!=null && FB_CONN_PARAMETERS.size()>0) {
                for (Map.Entry<String, String> kv : FB_CONN_PARAMETERS.entrySet()) {
                    connectionProperties.put(kv.getKey(), kv.getValue());
                }
            }
            if (properties!=null && properties.size()>0) {
                for (Map.Entry<String, String> kv : properties.entrySet()) {
                    if (connectionProperties.containsKey(kv.getKey())) {
                        connectionProperties.remove(kv.getKey());
                    }
                    connectionProperties.put(kv.getKey(), kv.getValue());
                }
            }
            _connection = _driver.connect(prepareConnectionString(), connectionProperties);
            System.out.println ("Connection established.");
            if (disableAutoCommit) {
                // Disable the default autocommit
                _connection.setAutoCommit(false);
                System.out.println("Auto-commit is disabled.");
            }
        } catch (SQLException se) {
            throw new FbSqlException(se);
        }
    }//open

    public void open() throws FbSqlException {
        open(false, null);
    }//open

    public void close() throws FbSqlException {
        try {
            if (_connection!=null && !_connection.isClosed()) {
                _connection.close();
            }
            _connection = null;
        } catch (SQLException se) {
            throw new FbSqlException(se);
        }
    }//close

    public void destroy() throws FbSqlException {
        FbSqlException e = null;
        try {
            if (_connection!=null && !_connection.isClosed()) {
                _connection.close();
            }
            _connection = null;
            if (_driver!=null) {
                _driver = null;
            }
        } catch (SQLException se) {
            _connection = null;
            _driver = null;
            e = new FbSqlException(se);
        }
        if (e!=null) {
            throw e;
        }
    }//destroy

    protected JsonArray processResultSet(ResultSet results) throws SQLException {
        if (results==null) {
            return null;
        }
        JsonArray result = new JsonArray();
        ResultSetMetaData resultsMetaData = results.getMetaData();
        while (results.next()) {
            JsonObject row = new JsonObject();
            for (int i=1; i<=resultsMetaData.getColumnCount(); i++) {
//                        System.out.println("Column: [" + resultsMetaData.getColumnName(i)
//                                + "] of type: [" + resultsMetaData.getColumnTypeName(i) + "]");
                if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("INTEGER")
                        || resultsMetaData.getColumnTypeName(i).toUpperCase().equals("SMALLINT")) {
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getInt(i));
                } else if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("BIGINT")) {
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getLong(i));
                } else if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("DECIMAL")) {
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getDouble(i));
                } else if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("BLOB SUB_TYPE 1")) {
                    Blob rcBlob = results.getBlob(i);
                    String rcBlobData = (rcBlob==null) ? null : rcBlob.toString();
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), rcBlobData);
                } else if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("TIMESTAMP")) {
//                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getTimestamp(i).getTime());
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getString(i));
                } else {
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getString(i));
                }
            }
            result.add(row);
        }
        return result;
    }//processResultSet

    public int executeQuery(String query) throws FbSqlException {
        if (isStringEmptyOrNull(query)) {
            throw new FbSqlException("Invalid query");
        }
        int result = -1;
        FbSqlException e = null;
        Statement statement = null;
        try {
            if (_connection==null || _connection.isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _connection.createStatement();
            statement.setQueryTimeout(timeout);
            result = statement.executeUpdate(query);
        } catch (SQLException se) {
            e = new FbSqlException(se);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
            } catch (NullPointerException | SQLException se) {
                statement = null;
            }
        }
        if (e!=null) {
            throw e;
        }
        return result;
    }//executeQuery

//    public int executeRepeatableQuery(String query, LinkedHashMap<String, Object> parameters) throws FbSqlException {
//        if (isStringEmptyOrNull(query)) {
//            throw new FbSqlException("Invalid query");
//        }
//        if (parameters==null || parameters.size()==0) {
//            throw new FbSqlException("Invalid query parameters");
//        }
//        int qryParams = StringUtils.countMatches(query, "?");
//        if (qryParams!=parameters.size()) {
//            throw new FbSqlException("Invalid query parameters count");
//        }
//        int result = -1;
//        FbSqlException e = null;
//        PreparedStatement statement = null;
//        try {
//            if (_connection==null || _connection.isClosed()) {
//                throw new FbSqlException("Invalid database connection");
//            }
//            statement = _connection.prepareStatement(query);
//            statement.setQueryTimeout(timeout);
//
//
//            stmt1.setString(1, "Doe");
//            stmt1.setString(2, "Joe");
//            result = statement.executeUpdate();
//
//
//        } catch (SQLException se) {
//            e = new FbSqlException(se);
//        } finally {
//            try {
//                if (statement != null) {
//                    statement.close();
//                    statement = null;
//                }
//            } catch (SQLException se) {
//                statement = null;
//            } catch (NullPointerException npe) {
//                statement = null;
//            }
//        }
//        if (e!=null) {
//            throw e;
//        }
//        return result;
//    }//executeQuery

    public JsonArray executeSelectQuery(String query) throws FbSqlException {
        if (isStringEmptyOrNull(query)) {
            throw new FbSqlException("Invalid query");
        }
        JsonArray result = null;
        FbSqlException e = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            if (_connection==null || _connection.isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _connection.createStatement();
            statement.setQueryTimeout(timeout);
            results = statement.executeQuery(query);
            result = processResultSet(results);
        } catch (SQLException se) {
            e = new FbSqlException(se);
        } finally {
            try {
                if (results!=null) {
                    results.close();
                    results = null;
                }
            } catch (NullPointerException | SQLException se) {
                results = null;
            }
            try {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
            } catch (NullPointerException | SQLException se) {
                statement = null;
            }
        }
        if (e!=null) {
            throw e;
        }
        return result;
    }//executeSelectQuery

    protected String prepareProcedureCallQuery(String procedure, int parametersCount) throws FbSqlException {
        if (isStringEmptyOrNull(procedure)) {
            throw new FbSqlException("Invalid stored procedure name");
        }
        StringBuilder qry = new StringBuilder();
        qry.append("{");
        qry.append("call ").append(procedure);
        if (parametersCount>0) {
            qry.append("(");
            for (int i=0; i<parametersCount; i++) {
                if (i>0) {
                    qry.append(",");
                }
                qry.append("?");
            }
            qry.append(")");
        }
        qry.append("}");
        return qry.toString();
    }//prepareProcedureCallQuery

    public boolean executeProcedure(String procedure, LinkedHashMap<String, Object> parameters) throws FbSqlException {
        if (isStringEmptyOrNull(procedure)) {
            throw new FbSqlException("Invalid stored procedure name");
        }
        boolean result = false;
        int parametersCount = parameters!=null ? parameters.size() : 0;
        FbSqlException e = null;
        CallableStatement statement = null;
        try {
            String callStr = prepareProcedureCallQuery(procedure, parametersCount);
            appLogger.info("executeProcedure [" + procedure + "] query: " + callStr);
            if (_connection == null || _connection.isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _connection.prepareCall(callStr);
            statement.setQueryTimeout(timeout);
            if (parametersCount > 0) {
                int i = 0;
                for (Map.Entry<String, Object> kv : parameters.entrySet()) {
                    i++;
                    if (kv.getValue() == null) {
                        statement.setNull(i, Types.NULL);
                    } else {
                        if (kv.getValue().getClass().getSimpleName().equals("Integer")) {
                            statement.setInt(i, (Integer) kv.getValue());
                        } else if (kv.getValue().getClass().getSimpleName().equals("Long")) {
                            statement.setLong(i, (Long) kv.getValue());
                        } else if (kv.getValue().getClass().getSimpleName().equals("Short")) {
                            statement.setLong(i, (Short) kv.getValue());
                        } else if (kv.getValue().getClass().getSimpleName().equals("Boolean")) {
                            statement.setBoolean(i, (Boolean) kv.getValue());
                        } else if (kv.getValue().getClass().getSimpleName().equals("Double")) {
                            statement.setDouble(i, (Double) kv.getValue());
                        } else if (kv.getValue().getClass().getName().equals("java.util.Date")) {
                            statement.setTimestamp(i, new Timestamp(((java.util.Date) kv.getValue()).getTime()));
                        } else if (kv.getValue().getClass().getName().equals("java.sql.Date")) {
                            statement.setTimestamp(i, new Timestamp(((java.sql.Date) kv.getValue()).getTime()));
                        } else if (kv.getValue().getClass().getSimpleName().equals("String")) {
                            statement.setString(i, kv.getValue().toString());
                        } else {
                            throw new FbSqlException("Invalid parameter type: [" + kv.getKey() + "] of type: " + kv.getValue().getClass().getSimpleName());
                        }
                    }
                }
            }
            statement.execute();
            result = true;
        } catch (ClassCastException cce) {
            e = new FbSqlException(cce);
        } catch (SQLException se) {
            e = new FbSqlException(se);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
            } catch (NullPointerException | SQLException se) {
                statement = null;
            }
        }
        if (e!=null) {
            throw e;
        }
        return result;
    }//executeQuery

    public JsonArray executeSelectProcedure(String procedure, LinkedHashMap<String, Object> parameters) throws FbSqlException {
        if (isStringEmptyOrNull(procedure)) {
            throw new FbSqlException("Invalid stored procedure name");
        }
        JsonArray result = null;
        FbSqlException e = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            if (_connection==null || _connection.isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _connection.createStatement();
            statement.setQueryTimeout(timeout);




//            result = processResultSet(results);
        } catch (SQLException se) {
            e = new FbSqlException(se);
        } finally {
            try {
                if (results!=null) {
                    results.close();
                    results = null;
                }
            } catch (NullPointerException | SQLException se) {
                results = null;
            }
            try {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
            } catch (NullPointerException | SQLException se) {
                statement = null;
            }
        }
        if (e!=null) {
            throw e;
        }
        return result;
    }//executeSelectProcedure

    public boolean hasTransactionsSupport() throws FbSqlException {
        if (_connection==null) {
            throw new FbSqlException("Database connection is closed");
        }
        try {
            DatabaseMetaData dbMetaData = _connection.getMetaData();
            return dbMetaData.supportsTransactions();
        } catch (SQLException se) {
            throw new FbSqlException(se);
        }
    }//hasTransactionsSupport

    /** Static generic helpers */
    public static boolean isStringEmptyOrNull(String input, boolean trimInput) {
        if(input == null) {
            return true;
        }
        if(trimInput) {
            return input.trim().length()==0;
        }
        return input.length()==0;
    }//isStringEmptyOrNull

    public static boolean isStringEmptyOrNull(String input) {
        return isStringEmptyOrNull(input, true);
    }//isStringEmptyOrNull
    /* END Static generic helpers */
}//FbSqlConnection
