package com.shadesframework.shadesdata;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
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
                ddlCreateQuery += columnName+" int";
            } else if (columnType.trim().equals("string")) {
                ddlCreateQuery += columnName+" varchar(255)";
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
        
        ArrayList<HashMap> rows = dataSet.getGeneratedRows();
        rows = CommonHelper.selectRandomItemsNoRepeat(rows, dataSet.howManyRowsToGenerate());
        
        for (HashMap row : rows) {
            HashMap rowClone = (HashMap)row.clone();
            rowClone.keySet().removeIf(value -> value.toString().matches("^@.*"));
            String insertSql = createInsertSql(dataSet, rowClone);
            logger.debug("insertSql => "+insertSql);
            runSql(insertSql);
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

    private void runSql(String sql) throws Exception {
        Connection conn = DriverManager.getConnection(jdbcUrl,"","");
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
        } finally {
            conn.close();
        }
               
    }
}