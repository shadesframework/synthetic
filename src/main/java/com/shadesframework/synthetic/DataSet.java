package com.shadesframework.synthetic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataSet implements Comparable <DataSet> {
    private static Logger logger = LogManager.getLogger(DataSet.class);

    private Storage storageHelper;
    private Metadata metaDataReader;
    private String name;
    private HashMap<String, HashMap> columns;
    private HashMap<String, ArrayList> relatedSets;
    private HashMap<String, String> storage;

    private HashMap<String, Object> generationContext = new HashMap();
    private HashMap<String, HashSet> uniqueValuesPerColumn = new HashMap();
    private boolean repeatedDataAdded = false;

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

    public Metadata getMetaReader() {
        return this.metaDataReader;
    }

    public HashMap<String, HashSet> getUniqueValuesPerColumn() {
        return this.uniqueValuesPerColumn;
    }

    public boolean isRepeatedDataAdded() {
        return this.repeatedDataAdded;
    }

    public void setRepeatedDataAdded(boolean added) {
        this.repeatedDataAdded = added;
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

    public DataSet getRelatedDataSetByName(String relatedDataSetName) throws Exception {
        if (relatedDataSetName == null) {
            throw new Exception("related data set name not given or null");
        }
        ArrayList<DataSet> relatedDataSets = this.getRelatedDataSets();
        for (DataSet dataSet : relatedDataSets) {
            if (dataSet.getName().trim().equals(relatedDataSetName.trim())) {
                return dataSet;
            }
        }
        return null;
    }

    public String getSpecificRelationParameter(DataSet relatedDataset, String relationParameter) throws Exception {
        ArrayList<String> wantedRelationType = 
        new ArrayList<String>( Arrays.asList("oneToOne", "oneToMany", "ManyToOne", "ManyToMany") );
        logger.debug("relatedDataSet => "+relatedDataset.getName());
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

    public String getThisColumn(DataSet relatedDataset) throws Exception {
        String thisColumn = getSpecificRelationParameter(relatedDataset, "thisColumn");
        String relationshipType = getRelationshipType(relatedDataset);
        if (relationshipType.trim().equals("oneToOne") || relationshipType.trim().equals("oneToMany") ) {
            if (!this.isColumnPrimaryKey(thisColumn)) {
                throw new Exception("this column ("+thisColumn+") needs to be set as primary key in ("+this.getName()+")");
            }
        } else {
            if (!this.isColumnForeignKey(thisColumn)) {
                throw new Exception("related column ("+thisColumn+") needs to be set as foreign key in ("+this.getName()+")");
            }
        }
        return thisColumn;
    }

    public ArrayList<DataSet> getRelatedDataSetsByThisColumnName(String columnName) throws Exception {
        ArrayList<DataSet> toReturn = new ArrayList();
        if (columnName == null) {
            throw new Exception("column name not given or null");
        }
        ArrayList<DataSet> relatedDataSets = this.getRelatedDataSets();
        for (DataSet relatedDataSet : relatedDataSets) {
            String relatedColumn = this.getThisColumn(relatedDataSet);
            if (relatedColumn.trim().equals(columnName.trim())) {
                toReturn.add(relatedDataSet);
            }
        }
        return toReturn;
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
    
    public ArrayList<String> generateRow(ArrayList<String> dataSetsAlreadyGeneratedRowsFor, HashMap parentRow, HashMap foreignKeyValues) throws Exception {

        generateRowLogger.debug("dataSetsAlreadyGeneratedRowsFor => "+dataSetsAlreadyGeneratedRowsFor);
        generateRowLogger.debug("parentRow => "+parentRow);
        generateRowLogger.debug("foreignKeyValues => "+foreignKeyValues);

        HashMap row = new LinkedHashMap();
        row.put("@dataset",this.getName());
        if (parentRow != null) {
            String parentDataSet = (String)parentRow.get("@dataset");
            if (parentDataSet == null) {
                throw new Exception("process dataset ("+this.getName()+") parent data set cannot be null ("+parentRow+")");
            }
            row.put("@parent-"+parentDataSet, parentRow);
        }
        
        if (dataSetsAlreadyGeneratedRowsFor == null) {
            dataSetsAlreadyGeneratedRowsFor = new ArrayList();
        } else {
            //dataSetsAlreadyGeneratedRowsFor = (ArrayList)dataSetsAlreadyGeneratedRowsFor.clone();
        }

        //if (foreignKeyValues != null) {
        foreignKeyValues = DataGenHelper.enrichForeignKeyValuesByAddingMissingValuesFromAllParents(this, dataSetsAlreadyGeneratedRowsFor, foreignKeyValues, row, false);
        //}
        
        if (!dataSetsAlreadyGeneratedRowsFor.contains(this.getName()) || (foreignKeyValues != null && foreignKeyValues.keySet().size() > 0)) { 
            ArrayList<String> columnNames = this.getColumns();
            for (String columnName : columnNames) {
                generateRowLogger.debug("\n===processing columnName ==="+this.getName()+"."+columnName);
                if (foreignKeyValues != null) {
                    if (foreignKeyValues.keySet().contains(columnName) 
                        && (this.getDataFormat(columnName) == null 
                        || this.getDataFormat(columnName).keySet().size() == 0)) {
                        row.put(columnName, foreignKeyValues.get(columnName));
                        generateRowLogger.debug("foreign key value set");
                        continue;
                    }
                }
                String dataType = this.getDataType(columnName);
                HashMap format = this.getDataFormat(columnName);
                
                generateRowLogger.debug("dataType ("+dataType+")");
                generateRowLogger.debug("format ("+format+")");

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
                            columnValue = DataGenHelper.generateNumber(this, dataSetsAlreadyGeneratedRowsFor, columnName,format, row, previousRow);
                        }
                        else if (dataType.toLowerCase().trim().equals("string")) {
                            columnValue = DataGenHelper.generateString(this, dataSetsAlreadyGeneratedRowsFor, columnName, format, row, previousRow);
                        } 
                        else if (dataType.toLowerCase().trim().equals("date")) {
                            columnValue = DataGenHelper.generateDate(columnName, format, row, previousRow);
                        } else {
                            throw new Exception("unrecognized datatype ("+dataType+") specified for column ("+this.getName()+"."+columnName+")");
                        }
                        if (!this.isColumnPrimaryKey(columnName)) {
                            isValueGeneratedCorrecly = true;
                            break;
                        } else {
                            // check if the value is not already generated for this column (being PK)
                            if (!this.isValueAlreadyPresentInPrimaryKey(columnName, columnValue)) {
                                generateRowLogger.debug("value not present in primary key from before");
                                isValueGeneratedCorrecly = true;
                                break;
                            }
                            else {
                                generateRowLogger.debug("value is present in primary key from before");
                            }
                        }
                        tryGeneratingUniqueValueCount++;
                    }
                    generateRowLogger.debug("isColumnPrimaryKey("+columnName+") => "+isColumnPrimaryKey(columnName));
                    generateRowLogger.debug("isValueGeneratedCorrecly => "+isValueGeneratedCorrecly);
                    if (this.isColumnPrimaryKey(columnName) && !isValueGeneratedCorrecly) {
                        generateRowLogger.debug("columnValue => "+columnValue);
                        generateRowLogger.debug("generatedRows => "+generatedRows);
                        generateRowLogger.error("unique value could not be generated for column ("+this.getName()+"."+columnName+")");
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
            generateRowLogger.debug("generatedRow => "+row);

            dataSetsAlreadyGeneratedRowsFor.add(this.getName());
            ArrayList<String> foreignKeys = this.getForeignKeys();

            
            // generate for related datasets now
            foreignKeyValues = new HashMap();
            for (String foreignKey : foreignKeys) {
                foreignKeyValues.put(foreignKey, row.get(foreignKey));
            }

            ArrayList<DataSet> relatedDataSets = this.getRelatedDataSets();
            for (DataSet relatedDataSet : relatedDataSets) {
                generateRowLogger.debug("relatedDataSet => "+relatedDataSet.getName());

                boolean generateRelatedSet = true;
                if (relatedDataSet.getGeneratedRows() != null 
                        && relatedDataSet.getGeneratedRows().size() == relatedDataSet.howManyRowsToGenerate()) {
                            generateRelatedSet = false;
                }
                generateRowLogger.debug("generateRelatedSet => "+generateRelatedSet);
                generateRowLogger.debug("dataSetsAlreadyGeneratedRowsFor.contains(relatedDataSet.getName()) => "+dataSetsAlreadyGeneratedRowsFor.contains(relatedDataSet.getName()));

                if (dataSetsAlreadyGeneratedRowsFor.contains(relatedDataSet.getName()) || !generateRelatedSet) {
                    generateRowLogger.debug("relatedDataSet ("+relatedDataSet+") is already genrated... no need to generate again ");
                    continue;
                }
                String relationshipType = this.getRelationshipType(relatedDataSet);
                
                String thisColumn = getSpecificRelationParameter(relatedDataSet, "thisColumn");
                String relatedColumn = getSpecificRelationParameter(relatedDataSet, "relatedColumn");

                generateRowLogger.debug("thisColumn => "+thisColumn);
                generateRowLogger.debug("relatedColumn => "+relatedColumn);

                if (relatedColumn == null) {
                    throw new Exception("related column ("+relatedColumn+") configuration missing for related data set ("+this.getName()+")");
                }
                if (!relatedDataSet.getColumns().contains(relatedColumn)) {
                    throw new Exception("related column ("+relatedColumn+") missing in related data set ("+relatedDataSet.getName()+")");
                }
                if (thisColumn != null) {
                    if (relationshipType.trim().toLowerCase().equals("onetoone") || 
                            relationshipType.trim().toLowerCase().equals("onetomany")) {
                        if (!this.isColumnPrimaryKey(thisColumn)) {
                            throw new Exception("thisColumn ("+thisColumn+") needs to be primary key for this data set ("+this.getName()+")");
                        }
                    } else if (relationshipType.trim().toLowerCase().equals("manytoone") || 
                    relationshipType.trim().toLowerCase().equals("manytomany")) {
                        if (!this.isColumnForeignKey(thisColumn)) {
                            throw new Exception("thisColumn ("+thisColumn+") needs to be foreign key for this data set ("+this.getName()+")");
                        }
                    }

                    if (row.keySet().contains(thisColumn)) {
                        foreignKeyValues.put(relatedColumn, row.get(thisColumn));        
                    } else {
                        throw new Exception("thisColumn ("+thisColumn+") is not present in this dataset ("+this.getName()+") so could not be set as foreign key for dataset ("+relatedDataSet.getName()+")");
                    }
                }
                else {
                    if (row.keySet().contains(relatedColumn)) {
                        foreignKeyValues.put(relatedColumn, row.get(relatedColumn));        
                    } else {
                        throw new Exception("relatedColumn ("+relatedColumn+") is not present in this dataset ("+this.getName()+") so could not be set as foreign key for dataset ("+relatedDataSet.getName()+")");
                    }
                }

                if (relationshipType != null) {
                    int multiplicity = 0;
                    if (relationshipType.trim().toLowerCase().equals("onetoone") || 
                            relationshipType.trim().toLowerCase().equals("manytoone")) {
                        if (!relatedDataSet.isColumnPrimaryKey(relatedColumn)) {
                            throw new Exception("relatedColumn ("+relatedColumn+") needs to be primary key for related data set ("+relatedDataSet.getName()+")");
                        }
                        multiplicity = 1;

                    } else if(relationshipType.trim().toLowerCase().equals("onetomany") || 
                        relationshipType.trim().toLowerCase().equals("manytomany")) {
                        if (!relatedDataSet.isColumnForeignKey(relatedColumn)) {
                            throw new Exception("relatedColumn ("+relatedColumn+") needs to be foreign key for related data set ("+relatedDataSet.getName()+")");
                        }
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
                    generateRowLogger.debug("======== from ("+this.getName()+") generating multiple for ("+relatedDataSet.getName()+") ====");
                    generateRowLogger.debug("multiplicity => "+multiplicity);
                    for (int i = 0 ; i < multiplicity ; i++) {
                        generateRowLogger.debug("======== multiplicity iteration ("+(i+1)+") for ("+relatedDataSet.getName()+") ====");
                        generateRowLogger.debug("dataSetsAlreadyGeneratedRowsFor => "+dataSetsAlreadyGeneratedRowsFor);
                        generateRowLogger.debug("foreignKeyValues => "+foreignKeyValues);
                        relatedDataSet.generateRow(dataSetsAlreadyGeneratedRowsFor, row, foreignKeyValues);
                    }
                }
            }
            // update uniqueValues per column
            updateUniqueValues(uniqueValuesPerColumn, row);
            // accumulate generated row
            generatedRows.add(row);
            // deposit this row as 'previousRow' before returning
            generationContext.put("previousRow", row);
            //return row;
        }
        else {
            generateRowLogger.debug("dataSetsAlreadyGeneratedRowsFor => "+dataSetsAlreadyGeneratedRowsFor);
            generateRowLogger.debug("already generated for ("+this.getName()+") so ignored");
        }
        return dataSetsAlreadyGeneratedRowsFor;
    }

    private static Logger generateRowLogger = LogManager.getLogger("generateRowLogger");

    public ArrayList<String> generateRows() throws Exception {
        return generateRows(new ArrayList());
    }

    public ArrayList<String> generateRows(ArrayList<String> avoidGenerationOfTheseDataSets) throws Exception {
        ArrayList<String> toReturn = new ArrayList();
        long rowsToGenerate = this.howManyRowsToGenerate();
        generateRowLogger.debug("\n\n\n===============generatteRows() for ("+this.getName()+")=========");
        generateRowLogger.debug("rowsToGenerate => "+rowsToGenerate);
        generateRowLogger.debug("avoidGenerationOfTheseDataSets => "+avoidGenerationOfTheseDataSets);

        ArrayList<String> fullyDoneDataSets = new ArrayList();
        if (avoidGenerationOfTheseDataSets != null) {
            for (String avoidedDataSet : avoidGenerationOfTheseDataSets) {
                if (!fullyDoneDataSets.contains(avoidedDataSet)) {
                    fullyDoneDataSets.add(avoidedDataSet);
                }
            }
        }
        generateRowLogger.debug("fullyDoneDataSets (start)=> "+fullyDoneDataSets);
        for (int i = 0 ; i < rowsToGenerate ; i++) { 
            generateRowLogger.debug("=== generatting row ("+(i+1)+") for ("+this.getName()+") =====");  
            ArrayList<String> generatedDataSets = this.generateRow((ArrayList)fullyDoneDataSets.clone(), null, null);
            generateRowLogger.debug("generatedDataSets => "+generatedDataSets);
            //generatedDataSets = (ArrayList)generatedDataSets.clone();
            generateRowLogger.debug("generatedDataSets clone => "+generatedDataSets);
            for (String generatedDataSetName : generatedDataSets) {
                if (!toReturn.contains(generatedDataSetName)) {
                    if (!generatedDataSetName.contains("-fully-done")) {
                        toReturn.add(generatedDataSetName);
                    }
                }
                if (generatedDataSetName.contains("-fully-done")) {
                    String fullyDoneDataSet = generatedDataSetName.substring(0, generatedDataSetName.indexOf("-fully-done"));
                    fullyDoneDataSets.add(fullyDoneDataSet);
                }
            }
            generateRowLogger.debug("toReturn => "+toReturn);
            generateRowLogger.debug("fullyDoneDataSets (end) => "+fullyDoneDataSets);
        }
        DataGenHelper.processRepeatForColumns(this);
        return toReturn;
    }

    public void storeRows() throws Exception {
        storageHelper.createDataSetContainer(this);
        storageHelper.storeRows(this);
    }

    public boolean isValueAlreadyPresentInPrimaryKey(String columnName, Object value) throws Exception {
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

    public void submitPrimaryKeyForColumn(String columnName, Object value) throws Exception {
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

    private void updateUniqueValues(HashMap<String, HashSet> uniqueValues, HashMap row) throws Exception {
        if (uniqueValues == null) {
            throw new Exception("uniqueValues cannot be passed as null");
        }
        if (row != null) {
            for (Object columnName : row.keySet()) {
                Object value = row.get(columnName);
                HashSet uniqueValueSet = uniqueValues.get(columnName);
                if (uniqueValueSet == null) {
                    uniqueValueSet = new HashSet();
                    uniqueValues.put((String)columnName, uniqueValueSet);
                }
                uniqueValueSet.add(value);
            }
        }
    }

    @Override
    public int compareTo(DataSet ds2)
    {
        try {
            ArrayList<DataSet> ds1ManySideSet = this.getRelatedDataSetsManySide();
            ArrayList<DataSet> ds1OneSideSet = this.getRelatedDataSetsOneSide();

            if (ds1ManySideSet.contains(ds2)) {
                return -1;
            }
            if (ds1OneSideSet.contains(ds2)) {
                return 1;
            }

            ArrayList<DataSet> ds2ManySideSet = ds2.getRelatedDataSetsManySide();
            ArrayList<DataSet> ds2OneSideSet = ds2.getRelatedDataSetsOneSide();

            if (ds2ManySideSet.contains(this)) {
                return 1;
            }
            if (ds2OneSideSet.contains(this)) {
                return -1;
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        
        return 0;
    }

    @Override
    public String toString() {
        return this.getName();
    }
}