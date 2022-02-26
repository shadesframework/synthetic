package com.shadesframework.shadesdata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class MetaDataHelper {
    public static boolean keyDeterminant(String dataSetName, HashMap<String, HashMap> columnsMeta, String columnName, String keyType) throws Exception {
        
        if (columnName == null) {
            throw new Exception("cannot pass null column name");
        }
        if (columnsMeta == null) {
            throw new Exception("columns not set for dataset ("+dataSetName+")");
        }
        else {
            HashMap<String, String> columnMeta = columnsMeta.get(columnName);
            if (columnMeta == null) {
                throw new Exception("there is no meta data for this column ("+columnName+")");
            }
            String assertion = columnMeta.get(keyType);
            return convertToBoolean(assertion, false);
        }
    }

    public static ArrayList<String> getKeys(String dataSetName, HashMap<String, HashMap> columnsMeta, String keyType) throws Exception {
        ArrayList<String> keys = new ArrayList();
        if (columnsMeta == null) {
            throw new Exception("columns not set for dataset ("+dataSetName+")");
        }
        else {
            for (String columnName : columnsMeta.keySet()) {
                HashMap<String, String> columnMeta = columnsMeta.get(columnName);
                if (columnMeta == null) {
                    throw new Exception("there is no meta data for this column ("+columnName+")");
                }
                String assertion = columnMeta.get(keyType);
                if (convertToBoolean(assertion, false)) {
                    keys.add(columnName);
                }
            }
        }
        return keys;
    }

    public static Object getDataTypeOrFormat(String dataSetName, String columnName, HashMap<String, HashMap> columnsMeta, String typeOrFormat) throws Exception {
        if (columnName == null) {
            throw new Exception("cannot pass null column name");
        }
        if (columnsMeta == null) {
            throw new Exception("columns not set for dataset ("+dataSetName+")");
        }
        else {
            HashMap<String, String> columnMeta = columnsMeta.get(columnName);
            if (columnMeta == null) {
                throw new Exception("there is no meta data for this column ("+columnName+")");
            }
            Object toReturn = columnMeta.get(typeOrFormat);
            return toReturn;
        }       
    }

    private static boolean convertToBoolean(String booleanAssertion, boolean defaultAssertion) throws Exception {
        if (booleanAssertion != null) {
            return Boolean.parseBoolean(booleanAssertion);
        }
        return defaultAssertion;
    }

    public static ArrayList<DataSet> getRelatedDataSets(Metadata metaDataReader, HashMap<String, ArrayList> relatedSets, ArrayList<String> wantedRelationType) throws Exception {
        ArrayList<DataSet> toReturn = new ArrayList();
        ArrayList<HashMap> relatedMetaSetList = getRelatedDataSetsMetaData(metaDataReader, relatedSets, wantedRelationType);
        for (HashMap<String, String> relatedSetMeta : relatedMetaSetList) {
            String relatedDataSetName = relatedSetMeta.get("relatedDataset");
            DataSet relatedDataSet = metaDataReader.getDataSet(relatedDataSetName);
            toReturn.add(relatedDataSet);
        }

        return toReturn;  
    }

    public static HashMap getRelatedDataSetMetaData(String relatedDatasetName, Metadata metaDataReader, HashMap<String, ArrayList> relatedSets, ArrayList<String> wantedRelationType) throws Exception {
        ArrayList<HashMap> relatedMetaSetList = getRelatedDataSetsMetaData(metaDataReader, relatedSets, wantedRelationType);
        for (HashMap<String, String> relatedSetMeta : relatedMetaSetList) {
            String relatedDataSetName = relatedSetMeta.get("relatedDataset");
            if (relatedDatasetName.trim().equals(relatedDataSetName.trim())) {
                return relatedSetMeta;
            }
        }
        return null;
    }

    public static ArrayList<HashMap> getRelatedDataSetsMetaData(Metadata metaDataReader, HashMap<String, ArrayList> relatedSets, ArrayList<String> wantedRelationType) throws Exception {
        ArrayList<HashMap> toReturn = new ArrayList();
        if (relatedSets != null) {
            for (String relationType : relatedSets.keySet()) {
                if (wantedRelationType.contains(relationType)) {
                    ArrayList<HashMap> relatedMetaSetList = relatedSets.get(relationType);
                    for (HashMap<String, String> relatedSetMeta : relatedMetaSetList) {
                        relatedSetMeta.put("relationType",relationType);
                        toReturn.add(relatedSetMeta);
                    }
                }
            }
        }  
        return toReturn;
    }

    public static Object getColumnFormatParameterValue(String parameter, HashMap format, HashMap row) throws Exception {
        if (format != null && parameter != null) {
            Object value = format.get(parameter);
            if (value instanceof Collection) {
                for (Map map : ((Collection<Map>)value)) {
                    String expression = ((HashMap<String, String>)map).get("expression");
                    Object expressionValue = evaluateExpression(expression, row);
                    if (!(expressionValue instanceof Boolean)) {
                        throw new Exception("expressionValue needed to be boolean => ("+expressionValue+")");
                    }
                    if (((Boolean)expressionValue)) {
                        String parameterValue = ((HashMap<String, String>)map).get("parameterValue"); 
                        return parameterValue;   
                    }
                }
            } else {
                return value.toString();
            }
        }
        return null;
    }

    public static Object evaluateExpression(String expression, Object context) throws Exception {
        if (context == null) {
            throw new Exception("context cannot be null");
        }
        if (expression == null) {
            throw new Exception("expression cannot be null");
        }
        ExpressionParser parser = new SpelExpressionParser();
        EvaluationContext evalcontext = new StandardEvaluationContext(context);    
        //evalcontext.addPropertyAccessor(new MapAccessor());    
        Expression exp = parser.parseExpression(expression);
        Object eval  = exp.getValue(evalcontext);
        return eval;
    }
}