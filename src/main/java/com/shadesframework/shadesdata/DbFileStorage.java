package com.shadesframework.shadesdata;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DbFileStorage implements Storage {
    private static Logger logger = LogManager.getLogger(DbFileStorage.class);
    String jdbcUrl = "";

    public DbFileStorage(String storageLocation) {
        this.jdbcUrl = storageLocation;
        if (this.jdbcUrl == null || this.jdbcUrl.trim().equals("")) {
            this.jdbcUrl = "jdbc:h2:file:~/test";
        }
    }
    
    @Override
    public void createDataSetContainer(DataSet dataSet) throws Exception {
        logger.debug("creating container for dataset ("+dataSet+")");
        
        String ddlDeleteQuery = "drop table if exists "+dataSet.getName();
        try {
            runSql(ddlDeleteQuery);
        }
        catch(Exception e) {
            // eat exception
        }
        
        String ddlCreateQuery = "create table "+dataSet.getName()+" (";
        for (int i = 0 ; i < dataSet.getColumns().size(); i++) {
            String columnName = dataSet.getColumns().get(i);
            String columnType = dataSet.getDataType(columnName);
            if (columnType.trim().equals("number")) {
                if (dataSet.getGeneratedRows().size() > 0) {
                    String numberStr = dataSet.getGeneratedRows().get(0).get(columnName).toString();
                    try {
                        Integer.parseInt(numberStr);
                        ddlCreateQuery += columnName+" int";
                    } catch(Exception e) {
                        try {
                            Double.parseDouble(numberStr);
                            ddlCreateQuery += columnName+" decimal(10,2)";
                        } catch(Exception ee) {
                            throw new Exception("data set ("+dataSet+") row ("+dataSet.getGeneratedRows().get(0)+") has unrecognizable number format ("+numberStr+")");   
                        }
                    }
                }
                
            } else if (columnType.trim().equals("string")) {
                ddlCreateQuery += columnName+" varchar(255)";
            } else if (columnType.trim().equals("date")) {
                ddlCreateQuery += columnName+" date";
            } else {
                throw new Exception("unrecognized datatype ("+columnType+") for column ("+dataSet.getName()+"."+columnName+")");
            }
            if (i < (dataSet.getColumns().size() - 1)) {
                ddlCreateQuery += ",";
            }
        }
        ddlCreateQuery += ")";
        logger.debug("ddlCreateQuery => "+ddlCreateQuery);

        runSql(ddlCreateQuery);
    }

    @Override
    public void storeRows(DataSet dataSet) throws Exception {
        try {
            ArrayList<HashMap> rows = dataSet.getGeneratedRows();
            rows = CommonHelper.selectRandomItemsNoRepeat(rows, dataSet.howManyRowsToGenerate());
            
            for (HashMap row : rows) {
                HashMap rowClone = (HashMap)row.clone();
                rowClone.keySet().removeIf(value -> value.toString().matches("^@.*"));
                String insertSql = createInsertSql(dataSet, rowClone);
                logger.debug("insertSql => "+insertSql);
                runSql(insertSql);
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    private String createInsertSql(DataSet dataSet, HashMap row) throws Exception {
        String ddlInsertQuery = "insert into "+dataSet.getName()+" values (";
        for (int i = 0 ; i < dataSet.getColumns().size(); i++) {
            String columnName = dataSet.getColumns().get(i);
            String columnType = dataSet.getDataType(columnName);
            if (columnType.trim().equals("number")) {
                ddlInsertQuery += row.get(columnName);
            } else if (columnType.trim().equals("string")) {
                ddlInsertQuery += "'"+row.get(columnName)+"'";
            } else if (columnType.trim().equals("date")) {
                ddlInsertQuery += "'"+new java.sql.Date(((Date)row.get(columnName)).getTime())+"'";
            } else {
                throw new Exception("unrecognized datatype ("+columnType+") for column ("+dataSet.getName()+"."+columnName+")");
            }
            if (i < (dataSet.getColumns().size() - 1)) {
                ddlInsertQuery += ",";
            }
        }
        ddlInsertQuery += ")";
        return ddlInsertQuery;
    }

    private static Connection conn = null;

    private void runSql(String sql) throws Exception {
        if (conn == null || conn.isClosed()) {
            if (jdbcUrl.contains("h2")) {
                Class.forName("org.h2.Driver");
            } else if (jdbcUrl.contains("sqlserver")) {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            }
            conn = DriverManager.getConnection(jdbcUrl);
        }
        
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
            /*
            ResultSet rs;
            rs = stmt.executeQuery(ddlQuery);
            
            while ( rs.next() ) {
                String lastName = rs.getString("Lname");
                System.out.println(lastName);
            }
            */
        }
        catch(Exception e) {
            e.printStackTrace();
            throw e;
        }      
    }
}