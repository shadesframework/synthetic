package com.shadesframework.shadesdata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;


public class MetaDataHelper {
    
    private static Logger logger = LogManager.getLogger(MetaDataHelper.class);

    public static boolean keyDeterminant(String dataSetName, HashMap<String, HashMap> columnsMeta, String columnName, String keyType) throws Exception {
        logger.debug("datasetName ("+dataSetName+") columnsMeta ("+columnsMeta+")");
        if (columnName == null) {
            throw new Exception("cannot pass null column name");
        }
        if (columnsMeta == null) {
            throw new Exception("columns not set for dataset ("+dataSetName+")");
        }
        else {
            HashMap<String, Object> columnMeta = columnsMeta.get(columnName);
            if (columnMeta == null) {
                throw new Exception("there is no meta data for this column ("+columnName+")");
            }
            String assertion = (String)columnMeta.get(keyType);
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
                HashMap<String, Object> columnMeta = columnsMeta.get(columnName);
                if (columnMeta == null) {
                    throw new Exception("there is no meta data for this column ("+columnName+")");
                }
                String assertion = (String)columnMeta.get(keyType);
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
            HashMap<String, Object> columnMeta = columnsMeta.get(columnName);
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
        logger.debug("relatedMetaSetList => "+relatedMetaSetList);
        for (HashMap<String, String> relatedSetMeta : relatedMetaSetList) {
            String relatedDataSetName = relatedSetMeta.get("relatedDataset");
            logger.debug("relatedDataSetName => "+relatedDataSetName);
            DataSet relatedDataSet = metaDataReader.getDataSet(relatedDataSetName);
            if (relatedDataSet == null) {
                logger.warn("related data set ("+relatedDataSetName+") not found");
            } else {
                toReturn.add(relatedDataSet);
            }
        }
        logger.debug("toReturn => "+toReturn);
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
        logger.debug("format => "+format);
        if (format != null && parameter != null) {
            logger.debug("parameter => :"+parameter+":");
            Object value = format.get(parameter);
            logger.debug("value => "+value);
            if (value instanceof Collection) {
                if (CommonHelper.isCollectionOfType((Collection)value, "java.lang.String")) {
                    logger.debug("collection is of type string");
                    return value;
                }
                for (Map map : ((Collection<Map>)value)) {
                    String expression = ((HashMap<String, String>)map).get("expression");
                    Object expressionValue = evaluateExpression(expression, row);
                    if (!(expressionValue instanceof Boolean)) {
                        throw new Exception("expression ("+expression+") does not evaluate to boolean => ("+expressionValue+")");
                    }
                    if (((Boolean)expressionValue)) {
                        Object parameterValue = ((HashMap<String, String>)map).get("parameterValue"); 
                        logger.debug("parameter ("+parameter+") expressionParameterValue ("+parameterValue+")");
                        return parameterValue;   
                    }
                }
            } else {
                if (value != null) {
                    return value.toString();
                }
            }
        }
        return null;
    }

    private static Logger expressionEvalLogger = LogManager.getLogger("expressionEvalLogger");
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
        expressionEvalLogger.debug("expression ("+expression+") context ("+context+") eval("+eval+")");
        return eval;
    }
}