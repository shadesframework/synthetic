package com.shadesframework.shadesdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataSet {
    private static Logger logger = LogManager.getLogger(DataSet.class);

    private Storage storageHelper;
    private Metadata metaDataReader;
    private String name;
    private HashMap<String, HashMap> columns;
    private HashMap<String, ArrayList> relatedSets;
    private HashMap<String, String> storage;

    private HashMap<String, Object> generationContext = new HashMap();

    ArrayList<HashMap> intermediateRows = new ArrayList();
    ArrayList<HashMap> generatedRows = new ArrayList();
    
    public DataSet(String name, Storage storageHelper, Metadata reader, 
        HashMap columns, HashMap relatedSets, HashMap storage) {
            this.name = name;
            this.storageHelper = storageHelper;
            this.metaDataReader = reader;
            this.columns = columns;
            this.relatedSets = relatedSets;
            this.storage = storage;
    }

    public  ArrayList<HashMap> getGeneratedRows() {
        return this.generatedRows;
    }

    public String getName() {
        return name;
    }

    public ArrayList<String> getColumns() throws Exception {
        if (columns != null) {
            Object keys = columns.keySet();
            return new ArrayList((Set)keys);
        }
        throw new Exception("columns not set for this dataset ("+this.name+")");
    }

    public boolean isColumnPrimaryKey(String columnName) throws Exception {
        return MetaDataHelper.keyDeterminant(this.getName(), columns, columnName, "isPrimaryKey");  
    }

    public boolean isColumnForeignKey(String columnName) throws Exception {
        return MetaDataHelper.keyDeterminant(this.getName(), columns, columnName, "isForeignKey"); 
    }

    public ArrayList<String> getPrimaryKeys() throws Exception {
        return MetaDataHelper.getKeys(this.getName(), columns, "isPrimaryKey");
    }

    public ArrayList<String> getForeignKeys() throws Exception {
        return MetaDataHelper.getKeys(this.getName(), columns, "isForeignKey");
    }

    
    public String getDataType(String columnName) throws Exception {
        return (String)MetaDataHelper.getDataTypeOrFormat(this.getName(), columnName, columns, "datatype");
    }

    public HashMap getDataFormat(String columnName) throws Exception {
        return (HashMap)MetaDataHelper.getDataTypeOrFormat(this.getName(), columnName, columns, "format");
    }

    public ArrayList<DataSet> getRelatedDataSets() throws Exception {
        ArrayList<String> wantedRelationType = 
            new ArrayList<String>( Arrays.asList("oneToOne", "oneToMany", "ManyToOne", "ManyToMany") );
        return MetaDataHelper.getRelatedDataSets(metaDataReader, relatedSets, wantedRelationType);
    }

    public ArrayList<DataSet> getRelatedDataSetsManySide() throws Exception {
        ArrayList<String> wantedRelationType = 
            new ArrayList<String>( Arrays.asList("oneToMany", "ManyToMany") );
        return MetaDataHelper.getRelatedDataSets(metaDataReader, relatedSets, wantedRelationType);
    }

    public ArrayList<DataSet> getRelatedDataSetsOneSide() throws Exception {
        ArrayList<String> wantedRelationType = 
            new ArrayList<String>( Arrays.asList("oneToOne", "ManyToOne") );
        return MetaDataHelper.getRelatedDataSets(metaDataReader, relatedSets, wantedRelationType);
    }

    private String getSpecificRelationParameter(DataSet relatedDataset, String relationParameter) throws Exception {
        ArrayList<String> wantedRelationType = 
        new ArrayList<String>( Arrays.asList("oneToOne", "oneToMany", "ManyToOne", "ManyToMany") );
        logger.debug("relatedDataSet => "+relatedDataset);
        HashMap<String, String> relatedSetMeta = MetaDataHelper.getRelatedDataSetMetaData(relatedDataset.getName(), metaDataReader, relatedSets, wantedRelationType);
        if (relatedSetMeta != null) {
            String specicParameterValue = relatedSetMeta.get(relationParameter);
            if (specicParameterValue != null) {
                return specicParameterValue.trim();
            }
            throw new Exception("parameter ("+relationParameter+") not configured for related data set ("+relatedDataset.getName()+")");
        }
        throw new Exception("no relation meta data configured for ("+relatedDataset.getName()+")");
    }

    public String getRelatedColumn(DataSet relatedDataset) throws Exception {
        String relatedColumn = getSpecificRelationParameter(relatedDataset, "relatedColumn");
        String relationshipType = getRelationshipType(relatedDataset);
        if (relationshipType.trim().equals("oneToOne") || relationshipType.trim().equals("ManyToOne") ) {
            if (!relatedDataset.isColumnPrimaryKey(relatedColumn)) {
                throw new Exception("related column ("+relatedColumn+") needs to be set as primary key in ("+relatedDataset.getName()+")");
            }
        } else {
            if (!relatedDataset.isColumnForeignKey(relatedColumn)) {
                throw new Exception("related column ("+relatedColumn+") needs to be set as foreign key in ("+relatedDataset.getName()+")");
            }
        }
        return relatedColumn;
    }

    public String getRelationshipType(DataSet relatedDataset) throws Exception {
        return getSpecificRelationParameter(relatedDataset, "relationType");
    }

    public long howManyRowsToGenerate() {
        String rows = storage.get("rows");
        if (rows != null) {
            return Long.parseLong(rows);
        }
        return 10L;
    }

    public boolean isDatatypeAndFormatSame(String columnName, DataSet relatedDataset, String relatedColumnName) throws Exception {
        if (!this.getDataType(columnName.trim()).trim().equals(relatedDataset.getDataType(relatedColumnName.trim()).trim())) {
            return false;
        }
        if (!this.getDataFormat(columnName.trim()).equals(relatedDataset.getDataFormat(relatedColumnName.trim()))) {
            return false;
        }
        return true;
    }

    public void submitIntermediateRow(HashMap row) throws Exception {
        intermediateRows.add(row);
    }

    public void clearIntermediateRows() throws Exception {
        intermediateRows.clear();
    }

    public void promoteIntermediateRows() throws Exception {
        generatedRows.addAll(intermediateRows);
    }

    public void applyLimitToGeneratedRows(long rowsUpperBound) throws Exception {
        // to be implemented
    }
    
    public HashMap generateRow(ArrayList<String> dataSetsAlreadyGeneratedRowsFor, HashMap foreignKeyValues) throws Exception {
        HashMap row = new HashMap();
        if (dataSetsAlreadyGeneratedRowsFor == null) {
            dataSetsAlreadyGeneratedRowsFor = new ArrayList();
        }
        
        if (!dataSetsAlreadyGeneratedRowsFor.contains(this.getName())) { // already covered ignore
            ArrayList<String> columnNames = this.getColumns();
            for (String columnName : columnNames) {
                if (foreignKeyValues != null) {
                    if (foreignKeyValues.keySet().contains(columnName)) {
                        row.put(columnName, foreignKeyValues.get(columnName));
                        continue;
                    }
                }
                String dataType = this.getDataType(columnName);
                HashMap format = this.getDataFormat(columnName);
                if (format == null) {
                    throw new Exception("format cannot be null for column ("+this.getName()+"."+columnName+")");
                }
                Object columnValue = null;
                HashMap previousRow =null;

                if (generationContext.containsKey("previousRow")) {
                    previousRow = (HashMap)generationContext.get("previousRow");   
                }

                int tryGeneratingUniqueValueCount = 0;
                boolean isValueGeneratedCorrecly = false;
                if (dataType != null) {
                    while (tryGeneratingUniqueValueCount < 3) {
                        if (dataType.toLowerCase().trim().equals("number")) {
                            columnValue = DataGenHelper.generateNumber(columnName,format, row, previousRow);
                        }
                        else if (dataType.toLowerCase().trim().equals("string")) {
                            columnValue = DataGenHelper.generateString(columnName, format, row, previousRow);
                        } else {
                            throw new Exception("unrecognized datatype ("+dataType+") specified for column ("+this.getName()+"."+columnName+")");
                        }
                        if (!this.isColumnPrimaryKey(columnName)) {
                            isValueGeneratedCorrecly = true;
                            break;
                        } else {
                            // check if the value is not already generated for this column (being PK)
                            if (!this.isValueAlreadyPresentInPrimaryKey(columnName, columnValue)) {
                                logger.debug("value not present in primary key from before");
                                isValueGeneratedCorrecly = true;
                                break;
                            }
                            else {
                                logger.debug("value is present in primary key from before");
                            }
                        }
                        tryGeneratingUniqueValueCount++;
                    }
                    logger.debug("isColumnPrimaryKey(columnName) => "+isColumnPrimaryKey(columnName));
                    logger.debug("isValueGeneratedCorrecly => "+isValueGeneratedCorrecly);
                    if (this.isColumnPrimaryKey(columnName) && !isValueGeneratedCorrecly) {
                        logger.debug("columnValue => "+columnValue);
                        logger.debug("generatedRows => "+generatedRows);
                        logger.error("unique value could not be generated for column ("+this.getName()+"."+columnName+")");
                        throw new Exception("unique value could not be generated for column ("+this.getName()+"."+columnName+")");
                    }
                    row.put(columnName, columnValue);
                    if (this.isColumnPrimaryKey(columnName)) {
                        submitPrimaryKeyForColumn(columnName, columnValue);
                    }
                }  else {
                    throw new Exception("dataType cannot be null for column ("+this.getName()+"."+columnName+")");
                }
            }
            // all columns are generated for this row
            // generate for related datasets now
            dataSetsAlreadyGeneratedRowsFor.add(this.getName());
            ArrayList<String> foreignKeys = this.getForeignKeys();

            foreignKeyValues = new HashMap();
            for (String foreignKey : foreignKeys) {
                foreignKeyValues.put(foreignKey, row.get(foreignKey));
            }

            ArrayList<DataSet> relatedDataSets = this.getRelatedDataSets();
            for (DataSet relatedDataSet : relatedDataSets) {
                logger.debug("relatedDataSet => "+relatedDataSet);
                String relationshipType = this.getRelationshipType(relatedDataSet);
                if (relationshipType != null) {
                    int multiplicity = 0;
                    if (relationshipType.trim().toLowerCase().equals("onetoone") || 
                            relationshipType.trim().toLowerCase().equals("manytoone")) {

                        multiplicity = 1;

                    } else if(relationshipType.trim().toLowerCase().equals("onetomany") || 
                        relationshipType.trim().toLowerCase().equals("manytomany")) {
                        
                        String multiplicityInt = getSpecificRelationParameter(relatedDataSet, "multiplicityGuidance");
                        if (multiplicityInt != null) {
                            multiplicity = Integer.parseInt(multiplicityInt);
                        } else {
                            multiplicity = (int)((double)relatedDataSet.howManyRowsToGenerate()/this.howManyRowsToGenerate());
                            if (multiplicity < 1) {
                                multiplicity = 1;
                            }
                        }
                    } else {
                        throw new Exception("dataset ("+this.getName()+") has unrecognized relationship type ("+relationshipType+") for related data set ("+relatedDataSet.getName()+")");
                    }

                    for (int i = 0 ; i < multiplicity ; i++) {
                        relatedDataSet.generateRow(dataSetsAlreadyGeneratedRowsFor, foreignKeyValues);
                    }
                }
            }

            generatedRows.add(row);
            // deposit this row as 'previousRow' before returning
            generationContext.put("previousRow", row);
            return row;
        }
        return null;
    }

    public void generateRows() throws Exception {
        long rowsToGenerate = this.howManyRowsToGenerate();
                
        for (int i = 0 ; i < rowsToGenerate ; i++) {   
            this.generateRow(null, null);
        }
    }

    public void storeRows() throws Exception {
        // to be implemented
    }

    private boolean isValueAlreadyPresentInPrimaryKey(String columnName, Object value) throws Exception {
        HashMap<String, ArrayList> generatedPrimaryKeys = (HashMap)this.generationContext.get("primaryKeys");
        if (generatedPrimaryKeys != null) {
            ArrayList primaryKeysForColumn = generatedPrimaryKeys.get(columnName);
            logger.debug("primaryKeysForColumn => "+primaryKeysForColumn);
            if (primaryKeysForColumn != null && primaryKeysForColumn.contains(value)) {
                return true;
            }
        }
        return false;
    }

    private void submitPrimaryKeyForColumn(String columnName, Object value) throws Exception {
        HashMap<String, ArrayList> generatedPrimaryKeys = (HashMap)this.generationContext.get("primaryKeys");
        if (generatedPrimaryKeys == null) {
            generatedPrimaryKeys = new HashMap();
            this.generationContext.put("primaryKeys", generatedPrimaryKeys);
        }
        ArrayList primaryKeysForColumn = generatedPrimaryKeys.get(columnName);
        if (primaryKeysForColumn == null) {
            primaryKeysForColumn = new ArrayList();
            generatedPrimaryKeys.put(columnName, primaryKeysForColumn);
        }
        primaryKeysForColumn.add(value);
    }
}