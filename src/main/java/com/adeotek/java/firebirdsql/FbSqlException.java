package com.adeotek.java.firebirdsql;

import org.firebirdsql.jdbc.FBSQLException;

import java.sql.SQLException;

public class FbSqlException extends Throwable {
    protected String _message;
    protected String _sqlState = null;
    protected int _code = 0;

    public String getMessage() { return _message;  }
    public String getSqlState() { return _sqlState;  }
    public int getErrorCode() { return _code;  }

    public FbSqlException() {
        super();
        _message = super.getMessage();
    }//FbSqlException

    public FbSqlException(String message) {
        super(message);
        _message = message;
    }//FbSqlException

    public FbSqlException(String message, int code, String sqlState) {
        super(message);
        _message = message;
        _code = code;
        _sqlState = sqlState;
    }//FbSqlException

    public FbSqlException(String message, Throwable cause) {
        super(message, cause);
        _message = message;
    }//FbSqlException

    public FbSqlException(Throwable cause) {
        super(cause);
        _message = super.getMessage();
    }//FbSqlException

    public FbSqlException(SQLException cause) {
        super(cause);
        _message = processMessage(cause);
        _sqlState = cause.getSQLState();
        _code = cause.getErrorCode();
    }//FbSqlException

    protected String processMessage(SQLException ex) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        while (ex != null) {
            if(first) {
                first = false;
            } else {
                sb.append("\n");
            }
            sb.append("#").append(ex.getErrorCode()).append("# ");
            sb.append(ex.getMessage());
            sb.append(" (SQL State: ").append(ex.getSQLState()).append(")");
            ex = ex.getNextException();
        }
        if (first) {
            return null;
        }
        return sb.toString();
    }//processMessage

    protected String processMessage(FBSQLException ex) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        while (ex != null) {
            if(first) {
                first = false;
            } else {
                sb.append("\n");
            }
            sb.append("#").append(ex.getErrorCode()).append("# ");
            sb.append(ex.getMessage());
            sb.append(" (SQL State: ").append(ex.getSQLState()).append(")");
            ex = (FBSQLException) ex.getNextException();
        }
        if (first) {
            return null;
        }
        return sb.toString();
    }//processMessage
}//FbSqlException
