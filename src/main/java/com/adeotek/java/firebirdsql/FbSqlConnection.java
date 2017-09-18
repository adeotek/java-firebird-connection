package com.adeotek.java.firebirdsql;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.firebirdsql.jdbc.FirebirdCallableStatement;

import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class FbSqlConnection {
    protected static final Logger appLogger = LogManager.getLogger(FbSqlConnection.class);
    protected static final String FB_CONN_STR_PREFIX = "jdbc:firebirdsql:";
    // isc_tpb_wait + isc_tpb_lock_timeout=? / isc_tpb_nowait
    protected static final String FB_TRAN_DEFAULT_PROPERTIES = "isc_tpb_read_committed,isc_tpb_rec_version,isc_tpb_write,isc_tpb_wait,isc_tpb_lock_timeout=";
    protected static final Properties FB_CONN_PARAMETERS = GetDefaultConnectionParameters();
    protected static Properties GetDefaultConnectionParameters() {
        Properties params = new Properties();
        params.put("sqlDialect", "3");
        params.put("charSet", "UTF-8");
//        params.put("encoding", "UTF8");
        params.setProperty("TRANSACTION_READ_COMMITTED", FB_TRAN_DEFAULT_PROPERTIES + "10");
        params.put("defaultIsolation", "TRANSACTION_READ_COMMITTED");
        params.put("defaultTransactionIsolation", Connection.TRANSACTION_READ_COMMITTED);
        params.put("connectTimeout", 30);
        return params;
    }//CreateDefaultConnectionParameters

    protected String _driverName = "org.firebirdsql.jdbc.FBDriver";
    protected String _hostname = "localhost";
    protected int _port = 3050;
    protected String _username = "SYSDBA";
    protected String _password = "masterkey";
    protected String _database;
    protected int _lockTimeout = -1;
    protected String _connectionString = null;
    protected Driver _driver = null;
    protected String _defaultConnectionName = null;
    protected HashMap<String, Connection> _transactions = null;

    // Values: "type4"/"java"/null (default); "local"; "native"; "embedded"
    public String connectionType = null;
    public boolean logDuration = false;
    public void setLockTimeout(int timeout) { _lockTimeout = timeout; }

    public FbSqlConnection(String database, String hostname, int port, String username, String password, String driverName) {
        try {
            _defaultConnectionName = Helpers.uidHash("FbSqlDefaultConnection");
        } catch (Exception silent) {
            _defaultConnectionName = "FbSqlDefaultConnection" + System.currentTimeMillis();
        }
        _database = database;
        if (!Helpers.isStringEmptyOrNull(hostname)) {
            _hostname = hostname;
        }
        if (port>0) {
            _port = port;
        }
        if (!Helpers.isStringEmptyOrNull(username)) {
            _username = username;
        }
        if (password!=null) {
            _password = password;
        }
        if (!Helpers.isStringEmptyOrNull(driverName)) {
            _driverName = driverName;
        }
    }//FBConnection

    public FbSqlConnection(String database, String hostname, String username, String password) {
        this(database, hostname, 0, username, password, null);
    }//FBConnection

    public FbSqlConnection(String connectionString, String username, String password) {
        try {
            _defaultConnectionName = Helpers.uidHash("FbSqlDefaultConnection");
        } catch (Exception silent) {
            _defaultConnectionName = "FbSqlDefaultConnection" + System.currentTimeMillis();
        }
        _connectionString = connectionString;
        if (!Helpers.isStringEmptyOrNull(username)) {
            _username = username;
        }
        if (password!=null) {
            _password = password;
        }
    }//FBConnection

    protected String prepareConnectionString() {
        if (!Helpers.isStringEmptyOrNull(_connectionString)) {
            return _connectionString;
        }
        StringBuilder connStr = new StringBuilder();
        connStr.append(FB_CONN_STR_PREFIX);
        if (connectionType!=null && connectionType.trim().toLowerCase().equals("local")) {
            connStr.append("local").append(":");
        } else if (connectionType!=null && connectionType.trim().toLowerCase().equals("native")) {
            connStr.append("native").append(":");
        } else if (connectionType!=null && connectionType.trim().toLowerCase().equals("embedded")) {
            connStr.append("embedded").append(":");
        }
        connStr.append("//").append(_hostname).append(":").append(_port);
        connStr.append("/").append(_database);
        if (FB_CONN_PARAMETERS!=null && FB_CONN_PARAMETERS.size()>0) {
            boolean first = true;
            for (Map.Entry<Object, Object> kv : FB_CONN_PARAMETERS.entrySet()) {
                if(first) {
                    first = false;
                    connStr.append("?");
                } else {
                    connStr.append("&");
                }
                if (kv.getKey().toString().equals("TRANSACTION_READ_COMMITTED") && _lockTimeout>=0) {
                    connStr.append(kv.getKey().toString()).append("=").append(FB_TRAN_DEFAULT_PROPERTIES + _lockTimeout);
                } else {
                    connStr.append(kv.getKey().toString()).append("=").append(kv.getValue().toString());
                }
            }
        }
        return connStr.toString();
    }//prepareConnectionString

    protected void loadDriver(String driverName) throws FbSqlException {
        if (Helpers.isStringEmptyOrNull(driverName)) {
            throw new FbSqlException("Invalid driver name (null or empty)");
        }
        try {
            Class.forName(driverName);
            // We pass the entire database URL, but we could just pass "jdbc:interbase:"
            _driver = DriverManager.getDriver(prepareConnectionString());
            appLogger.info("Firebird JCA-JDBC driver version " +
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

    protected Connection connect(boolean disableAutoCommit, Properties properties) throws FbSqlException {
        long dsts = logDuration ? System.currentTimeMillis() : 0;
        loadDriver(_driverName);
        if (logDuration) {
            appLogger.info("loadDriver duration: " + Helpers.getDuration(dsts));
            dsts = System.currentTimeMillis();
        }
        Connection conn;
        try {
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", _username);
            connectionProperties.put("password", _password);
            if (FB_CONN_PARAMETERS!=null && FB_CONN_PARAMETERS.size()>0) {
                for (Map.Entry<Object, Object> kv : FB_CONN_PARAMETERS.entrySet()) {
                    if (kv.getKey().toString().equals("TRANSACTION_READ_COMMITTED") && _lockTimeout>=0) {
                        connectionProperties.put(kv.getKey(), FB_TRAN_DEFAULT_PROPERTIES + _lockTimeout);
                    } else {
                        connectionProperties.put(kv.getKey(), kv.getValue());
                    }
                }
            }
            if (properties!=null && properties.size()>0) {
                for (Map.Entry<Object, Object> kv : properties.entrySet()) {
                    if (connectionProperties.containsKey(kv.getKey())) {
                        connectionProperties.remove(kv.getKey());
                    }
                    connectionProperties.put(kv.getKey(), kv.getValue());
                }
            }
            conn = _driver.connect(prepareConnectionString(), connectionProperties);
            appLogger.info("Connection established...");
            if (disableAutoCommit) {
                // Disable the default autocommit
                conn.setAutoCommit(false);
                appLogger.info("Auto-commit is disabled");
            }
            checkDbWarnings(conn);
            if (logDuration) { appLogger.info("connect duration: " + Helpers.getDuration(dsts)); }
        } catch (SQLException se) {
            throw new FbSqlException(se);
        }
        return conn;
    }//connect

    protected void disconnect(String transaction) throws FbSqlException {
        if(Helpers.isStringEmptyOrNull(transaction)) {
            throw new FbSqlException("Invalid connection/transaction identifier");
        }
        try {
            if (_transactions.get(transaction)!=null && !_transactions.get(transaction).isClosed()) {
                _transactions.get(transaction).close();
            }
            _transactions.remove(transaction);
        } catch (SQLException se) {
            throw new FbSqlException(se);
        }
    }//disconnect

    protected void rollback(String transaction) throws FbSqlException {
        if(Helpers.isStringEmptyOrNull(transaction) || _transactions==null || !_transactions.containsKey(transaction) || _transactions.get(transaction)==null) {
            throw new FbSqlException("Invalid or inactive connection/transaction");
        }
        SQLException e = null;
        try {
            if (!_transactions.get(transaction).getAutoCommit()) {
                _transactions.get(transaction).rollback();
            }
        } catch (SQLException se) {
            e = se;
        } finally {
            try {
                if (!_transactions.get(transaction).getAutoCommit()) {
                    if (_transactions.get(transaction) != null && !_transactions.get(transaction).isClosed()) {
                        _transactions.get(transaction).close();
                    }
                    _transactions.remove(transaction);
                }
            } catch (SQLException se) {
                if (e==null) { e = se; }
                else { e.setNextException(se); }
            }
        }
        if (e!=null) {
            throw new FbSqlException(e);
        }
    }//rollback

    protected String getCurrentConnection(String transaction) throws FbSqlException {
        String tranName = Helpers.isStringEmptyOrNull(transaction) ? _defaultConnectionName : transaction;
        if (_transactions==null || !_transactions.containsKey(tranName)) {
            throw new FbSqlException("Invalid or inactive connection/transaction");
        }
        return tranName;
    }//getCurrentConnection

    protected void checkDbWarnings(String transaction) throws SQLException {
        String tran = Helpers.isStringEmptyOrNull(transaction) ? _defaultConnectionName : transaction;
        if (_transactions==null || !_transactions.containsKey(tran) || _transactions.get(tran)==null) {
            return;
        }
        SQLWarning warning = _transactions.get(tran).getWarnings();
        while (warning != null) {
            appLogger.warn(warning.getClass().getSimpleName() + ": " + warning.getMessage());
            warning = warning.getNextWarning();
        }
    }//checkDbWarnings

    protected void checkDbWarnings(Connection conn) throws SQLException {
        if (conn==null || !conn.isClosed()) {
            return;
        }
        SQLWarning warning = conn.getWarnings();
        while (warning != null) {
            appLogger.warn(warning.getClass().getSimpleName() + ": " + warning.getMessage());
            warning = warning.getNextWarning();
        }
    }//checkDbWarnings

    public void open(boolean disableAutoCommit, Properties properties) throws FbSqlException {
        if (_transactions!=null && _transactions.containsKey(_defaultConnectionName) && _transactions.get(_defaultConnectionName)!=null) {
            throw new FbSqlException("Connection already opened");
        }
        if (_transactions==null) {
            _transactions = new HashMap<>();
        } else if(_transactions.containsKey(_defaultConnectionName)) {
            _transactions.remove(_defaultConnectionName);
        }
        _transactions.put(_defaultConnectionName, connect(disableAutoCommit, properties));
    }//open

    public void open() throws FbSqlException {
        open(false, null);
    }//open

    public void close() throws FbSqlException {
        disconnect(_defaultConnectionName);
    }//close

    public void destroy() throws FbSqlException {
        SQLException e = null;
        if (_transactions!=null && _transactions.size()>0) {
            try {
                for(String tran : _transactions.keySet()) {
                    try {
                        if (_transactions.get(tran)!=null && !_transactions.get(tran).isClosed()) {
                            _transactions.get(tran).close();
                        }
                    } catch (SQLException se) {
                        if (e == null) { e = se; }
                        else { e.setNextException(se); }
                    } catch (Exception er) {
                        appLogger.error(er.getClass().getSimpleName() + ": " + er.getMessage());
                    }
                }
                _transactions.clear();
            } catch (Exception err) {
                appLogger.error(err.getClass().getSimpleName() + ": " + err.getMessage());
                _transactions = null;
            }
        }
        if (_driver!=null) {
            _driver = null;
        }
        if (e!=null) {
            throw new FbSqlException(e);
        }
    }//destroy

    public void beginTransaction(String transaction, boolean overwrite, Properties properties) throws FbSqlException {
        if(Helpers.isStringEmptyOrNull(transaction)) {
            throw new FbSqlException("Invalid transaction identifier");
        }
        if (_transactions!=null && _transactions.containsKey(transaction) && _transactions.get(transaction)!=null && !overwrite) {
            throw new FbSqlException("Transaction already started");
        }
        if (_transactions==null) {
            _transactions = new HashMap<>();
        } else if(_transactions.containsKey(transaction)) {
            _transactions.remove(transaction);
        }
        _transactions.put(transaction, connect(true, properties));
    }//beginTransaction

    public void beginTransaction(String transaction, boolean overwrite) throws FbSqlException {
        beginTransaction(transaction, overwrite, null);
    }//beginTransaction

    public void commitTransaction(String transaction, boolean keepOpened) throws FbSqlException {
        if(Helpers.isStringEmptyOrNull(transaction) || _transactions==null || !_transactions.containsKey(transaction) || _transactions.get(transaction)==null) {
            throw new FbSqlException("Invalid or inactive transaction");
        }
        try {
            _transactions.get(transaction).commit();
        } catch (SQLException se) {
            throw new FbSqlException(se);
        } finally {
            if (!keepOpened) {
                try {
                    if (_transactions.get(transaction)!=null && !_transactions.get(transaction).isClosed()) {
                        _transactions.get(transaction).close();
                    }
                    _transactions.remove(transaction);
                } catch (SQLException se) {
                    throw new FbSqlException(se);
                }
            }
        }
    }//commitTransaction

    public void commitTransaction(String transaction) throws FbSqlException {
        commitTransaction(transaction, false);
    }//commitTransaction

    public void rollbackTransaction(String transaction, boolean keepOpened) throws FbSqlException {
        if(Helpers.isStringEmptyOrNull(transaction) || _transactions==null || !_transactions.containsKey(transaction) || _transactions.get(transaction)==null) {
            throw new FbSqlException("Invalid or inactive transaction");
        }
        try {
            _transactions.get(transaction).rollback();
        } catch (SQLException se) {
            throw new FbSqlException(se);
        } finally {
            if (!keepOpened) {
                try {
                    if (_transactions.get(transaction)!=null && !_transactions.get(transaction).isClosed()) {
                        _transactions.get(transaction).close();
                    }
                    _transactions.remove(transaction);
                } catch (SQLException se) {
                    throw new FbSqlException(se);
                }
            }
        }
    }//rollbackTransaction

    public void rollbackTransaction(String transaction) throws FbSqlException {
        rollbackTransaction(transaction, false);
    }//rollbackTransaction

    public void closeTransaction(String transaction) throws FbSqlException {
        disconnect(transaction);
    }//closeTransaction

    protected PreparedStatement prepareParams(PreparedStatement statement, LinkedHashMap<String, Object> parameters) throws SQLException, FbSqlException {
        if (parameters==null || parameters.size()==0) {
            return statement;
        }
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
                    String val = kv.getValue().toString();
                    if (val.length()>4000) {
                        statement.setBlob(i, Helpers.stringToInputStream(val));
                    } else {
                        statement.setString(i, val);
                    }
                } else {
                    throw new FbSqlException("Invalid parameter type: [" + kv.getKey() + "] of type: " + kv.getValue().getClass().getSimpleName());
                }
            }
        }
        return statement;
    }//prepareParams

    protected CallableStatement prepareCallableParams(CallableStatement statement, LinkedHashMap<String, Object> parameters) throws SQLException, FbSqlException {
        if (parameters==null || parameters.size()==0) {
            return statement;
        }
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
        return statement;
    }//prepareCallableParams

    protected JsonArray processResultSet(ResultSet results) throws SQLException {
        if (results==null) {
            return null;
        }
        JsonArray result = new JsonArray();
        ResultSetMetaData resultsMetaData = results.getMetaData();
        while (results.next()) {
            JsonObject row = new JsonObject();
            for (int i=1; i<=resultsMetaData.getColumnCount(); i++) {
                appLogger.debug("Column: [" + resultsMetaData.getColumnName(i)
                                + "] of type: [" + resultsMetaData.getColumnTypeName(i) + "]");
                if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("INTEGER")
                        || resultsMetaData.getColumnTypeName(i).toUpperCase().equals("SMALLINT")) {
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getInt(i));
                } else if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("BIGINT")) {
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getLong(i));
                } else if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("DECIMAL")) {
                    row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), results.getDouble(i));
                } else if(resultsMetaData.getColumnTypeName(i).toUpperCase().equals("BLOB SUB_TYPE 1")) {
                    Blob rcBlob = results.getBlob(i);
                    if (rcBlob==null) {
                        row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), String.valueOf(null));
                    } else {
                        row.addProperty(resultsMetaData.getColumnName(i).toLowerCase(), Helpers.inputStreamToString(rcBlob.getBinaryStream()));
                    }
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

    public int executeQuery(String query, String transaction) throws FbSqlException {
        if (Helpers.isStringEmptyOrNull(query)) {
            throw new FbSqlException("Invalid query");
        }
        String tran = getCurrentConnection(transaction);
        long dsts = logDuration ? System.currentTimeMillis() : 0;
        int result = -1;
        FbSqlException e = null;
        Statement statement = null;
        try {
            if (_transactions.get(tran)==null || _transactions.get(tran).isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _transactions.get(tran).createStatement();
            result = statement.executeUpdate(query);
            checkDbWarnings(_transactions.get(tran));
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
            if (e!=null) {
                rollback(tran);
            }
        }
        if (e!=null) {
            throw e;
        }
        if (logDuration) { appLogger.info("executeQuery duration: " + Helpers.getDuration(dsts)); }
        return result;
    }//executeQuery

    public int executeQuery(String query) throws FbSqlException {
        return executeQuery(query, null);
    }//executeQuery

    public JsonArray executeSelectQuery(String query, String transaction) throws FbSqlException {
        if (Helpers.isStringEmptyOrNull(query)) {
            throw new FbSqlException("Invalid query");
        }
        String tran = getCurrentConnection(transaction);
        long dsts = logDuration ? System.currentTimeMillis() : 0;
        JsonArray result = null;
        FbSqlException e = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            if (_transactions.get(tran) == null || _transactions.get(tran).isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _transactions.get(tran).createStatement();
            results = statement.executeQuery(query);
            result = processResultSet(results);
            checkDbWarnings(_transactions.get(tran));
        } catch (ConcurrentModificationException cme) {
            e = new FbSqlException(cme);
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
            if (e!=null) {
                rollback(tran);
            }
        }
        if (e!=null) {
            throw e;
        }
        if (logDuration) { appLogger.info("executeSelectQuery duration: " + Helpers.getDuration(dsts)); }
        return result;
    }//executeSelectQuery

    public JsonArray executeSelectQuery(String query) throws FbSqlException {
        return executeSelectQuery(query, null);
    }//executeSelectQuery

    public int executeBulkInsert(String tableName, String[] fields, List<LinkedHashMap<String, Object>> values, String transaction) throws FbSqlException {
        if (Helpers.isStringEmptyOrNull(tableName)) {
            throw new FbSqlException("Invalid query");
        }
        if (fields==null || fields.length==0 || values==null || values.size()==0) {
            throw new FbSqlException("Invalid query parameters");
        }
        String tran = getCurrentConnection(transaction);
        long dsts = logDuration ? System.currentTimeMillis() : 0;
        int result = -1;
        FbSqlException e = null;
        PreparedStatement statement = null;
        try {
            if (_transactions.get(tran)==null || _transactions.get(tran).isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            String qryParams = "";
            String query = "INSERT INTO \"" + tableName + "\" (";
            boolean first = true;
            for (String key : fields) {
                if (first) {
                    first = false;
                } else {
                    query += ", ";
                    qryParams += ", ";
                }
                query += "\"" + key + "\"";
                qryParams += "?";
            }
            query += ") VALUES (" + qryParams + ")";
            statement = _transactions.get(tran).prepareStatement(query);

            int i = 0;
            for (LinkedHashMap<String, Object> parameters : values) {
                i++;
                if (parameters==null || parameters.size()!=fields.length) {
                    throw new FbSqlException("Invalid values at position:" + i);
                }
                statement = prepareParams(statement, parameters);
                statement.addBatch();
            }

            int[] resultsCounts = statement.executeBatch();
            result = 0;
            for(int res : resultsCounts) {
                if (res>0 || res==Statement.SUCCESS_NO_INFO) {
                    result++;
                }
            }
        } catch (SQLException se) {
            e = new FbSqlException(se);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
            } catch (SQLException se) {
                statement = null;
            } catch (NullPointerException npe) {
                statement = null;
            }
            if (e!=null) {
                rollback(tran);
            }
        }
        if (e!=null) {
            throw e;
        }
        if (logDuration) { appLogger.info("executeBulkInsert duration: " + Helpers.getDuration(dsts)); }
        return result;
    }//executeBulkInsert

    public int executeBulkInsert(String tableName, String[] fields, List<LinkedHashMap<String, Object>> values) throws FbSqlException {
        return executeBulkInsert(tableName, fields, values, null);
    }//executeBulkInsert

    protected String prepareProcedureCallQuery(String procedure, int parametersCount) throws FbSqlException {
        if (Helpers.isStringEmptyOrNull(procedure)) {
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

    public boolean executeProcedure(String procedure, LinkedHashMap<String, Object> parameters, String transaction) throws FbSqlException {
        if (Helpers.isStringEmptyOrNull(procedure)) {
            throw new FbSqlException("Invalid stored procedure name");
        }
        String tran = getCurrentConnection(transaction);
        long dsts = logDuration ? System.currentTimeMillis() : 0;
        boolean result = false;
        int parametersCount = parameters!=null ? parameters.size() : 0;
        FbSqlException e = null;
        CallableStatement statement = null;
        try {
            String callStr = prepareProcedureCallQuery(procedure, parametersCount);
            appLogger.info("executeProcedure [" + procedure + "] query: " + callStr);
            if (_transactions.get(tran) == null || _transactions.get(tran).isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _transactions.get(tran).prepareCall(callStr);
            if (parametersCount>0) {
                statement = prepareCallableParams(statement, parameters);
            }
            statement.execute();
            checkDbWarnings(_transactions.get(tran));
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
            if (e!=null) {
                rollback(tran);
            }
        }
        if (e!=null) {
            throw e;
        }
        if (logDuration) { appLogger.info("executeProcedure duration: " + Helpers.getDuration(dsts)); }
        return result;
    }//executeProcedure

    public boolean executeProcedure(String procedure, LinkedHashMap<String, Object> parameters) throws FbSqlException {
        return executeProcedure(procedure, parameters, null);
    }//executeProcedure

    public JsonArray executeSelectableProcedure(String procedure, LinkedHashMap<String, Object> parameters, String transaction) throws FbSqlException {
        if (Helpers.isStringEmptyOrNull(procedure)) {
            throw new FbSqlException("Invalid stored procedure name");
        }
        String tran = getCurrentConnection(transaction);
        long dsts = logDuration ? System.currentTimeMillis() : 0;
        JsonArray result = null;
        int parametersCount = parameters!=null ? parameters.size() : 0;
        FbSqlException e = null;
        CallableStatement statement = null;
        ResultSet results = null;
        try {
            String callStr = prepareProcedureCallQuery(procedure, parametersCount);
            appLogger.info("executeProcedure [" + procedure + "] query: " + callStr);
            if (_transactions.get(tran) == null || _transactions.get(tran).isClosed()) {
                throw new FbSqlException("Invalid database connection");
            }
            statement = _transactions.get(tran).prepareCall(callStr);
            FirebirdCallableStatement fbStatement = (FirebirdCallableStatement) statement;
            fbStatement.setSelectableProcedure(true);
            if (parametersCount>0) {
                statement = prepareCallableParams(statement, parameters);
            }
            fbStatement = (FirebirdCallableStatement) statement;
            appLogger.debug("isSelectableProcedure: " + fbStatement.isSelectableProcedure());
            results = statement.executeQuery();
            checkDbWarnings(_transactions.get(tran));
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
            if (e!=null) {
                rollback(tran);
            }
        }
        if (e!=null) {
            throw e;
        }
        if (logDuration) { appLogger.info("executeSelectableProcedure duration: " + Helpers.getDuration(dsts)); }
        return result;
    }//executeSelectableProcedure

    public JsonArray executeSelectableProcedure(String procedure, LinkedHashMap<String, Object> parameters) throws FbSqlException {
        return executeSelectableProcedure(procedure, parameters, null);
    }//executeSelectableProcedure

    public boolean hasTransactionsSupport(String transaction) throws FbSqlException {
        String tran = getCurrentConnection(transaction);
        try {
            if (_transactions.get(tran)==null || _transactions.get(tran).isClosed()) {
                throw new FbSqlException("Database connection is closed");
            }
            DatabaseMetaData dbMetaData = _transactions.get(tran).getMetaData();
            return dbMetaData.supportsTransactions();
        } catch (SQLException se) {
            throw new FbSqlException(se);
        }
    }//hasTransactionsSupport
}//FbSqlConnection
