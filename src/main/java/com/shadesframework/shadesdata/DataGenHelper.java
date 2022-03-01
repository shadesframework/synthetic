package com.shadesframework.shadesdata;
import com.mifmif.common.regex.Generex;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataGenHelper {
    private static Logger logger = LogManager.getLogger(DataGenHelper.class);

    public static String generateString(String columnName, HashMap format, HashMap row, HashMap previousRow) throws Exception {
        String generatedString = null;
        Object regExp = MetaDataHelper.getColumnFormatParameterValue("regexBased",format, row);
        if (regExp != null) {
            generatedString = generateRandomString((String)regExp);
            return generatedString;
        }

        Object randomPick = MetaDataHelper.getColumnFormatParameterValue("randomPick",format, row);
        if (randomPick != null) {
            generatedString = pickRandomStringFromList((ArrayList<String>)randomPick);
            return generatedString;
        }

        Object rotate = MetaDataHelper.getColumnFormatParameterValue("rotate",format, row);
        if (rotate != null) {
            generatedString = pickNextStringFromList((ArrayList<String>)rotate, columnName, previousRow);
            return generatedString;
        }

        throw new Exception("don't know how to generate string for column ("+columnName+")");
    }

    private static String generateRandomString(String regExp) throws Exception {
        logger.debug("regExp => "+regExp);
        Generex generex = new Generex(regExp);
		String randomStr = generex.random();
        logger.debug("randomStr => "+randomStr);
        return randomStr;
    }

    private static String pickRandomStringFromList(ArrayList<String> stringList) throws Exception {
        int randomIndex = ThreadLocalRandom.current().nextInt(0, stringList.size());
        return stringList.get(randomIndex);
    }

    private static String pickNextStringFromList(ArrayList<String> stringList, String columnName, HashMap previousRow) throws Exception {
        if (previousRow == null) {
            return stringList.get(0);
        }
        String previousRowString = (String)previousRow.get(columnName);
        int indexToPick = 0;
        if (previousRowString != null) {
            indexToPick = stringList.indexOf(previousRowString) + 1;
            if (indexToPick > (stringList.size()-1)) {
                indexToPick = 0;
            }
        }
        return stringList.get(indexToPick);
    }
    
    private static boolean doesNumberDependOnPreviousRow(HashMap format, HashMap row) throws Exception {
        Object spacing = MetaDataHelper.getColumnFormatParameterValue("spacingFromPreviousRow",format, row);
        if (spacing == null) {
            return false;
        } else {
            return true;
        }
    }

    private static boolean isNumberRangeBased(HashMap format, HashMap row) throws Exception {
        Object rangeStart = MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row);
        if (rangeStart == null) {
            return false;
        } else {
            return true;
        }
    }

    private static boolean isNumberSynthetic(HashMap format, HashMap row) throws Exception {
        Object digitsBeforeDecimal = MetaDataHelper.getColumnFormatParameterValue("digitsBeforeDecimal",format, row);
        if (digitsBeforeDecimal == null) {
            return false;
        } else {
            return true;
        }
    }

    public static Number generateNumber(String columnName, HashMap format, HashMap row, HashMap previousRow) throws Exception {
        if (doesNumberDependOnPreviousRow(format, row) && previousRow != null) {
            logger.debug("number depends on previous row");
            Number number = (Number)previousRow.get(columnName);
            if (number == null) {
                throw new Exception("number doesnt exist for previous row");
            }
            double spacing = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("spacingFromPreviousRow",format, row));
            boolean randomSpacing = Boolean.parseBoolean((String)MetaDataHelper.getColumnFormatParameterValue("randomSpacing",format, row));
            String increment = (String)MetaDataHelper.getColumnFormatParameterValue("applySpacingWithIncrement",format, row);
            
            double rangeStart = -1.09090;
            Object rangeStartObj = MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row);
            if (rangeStartObj != null) {
                rangeStart = Double.parseDouble((String)rangeStartObj);
            }

            double rangeEnd = -1.09090;
            Object rangeEndObj = MetaDataHelper.getColumnFormatParameterValue("rangeEnd",format, row);
            if (rangeEndObj != null) {
                rangeEnd = Double.parseDouble((String)rangeEndObj);
            }

            int digitsAfterDecimal = 0;
            Object digitsAfterDecimalObject = MetaDataHelper.getColumnFormatParameterValue("digitsAfterDecimal",format, row);            
            if (digitsAfterDecimalObject != null) {
                digitsAfterDecimal = Integer.parseInt((String)digitsAfterDecimalObject);
            }

            double genenratedNumber = generateNumberWithSpacing(number.doubleValue(), spacing, randomSpacing, increment, rangeStart, rangeEnd);
            
            logger.debug("genenratedNumber =>"+genenratedNumber);
            logger.debug("digitsAfterDecimalObject => "+digitsAfterDecimalObject);
            if (digitsAfterDecimalObject != null) {
                logger.debug("digitsAfterDecimal => "+digitsAfterDecimal);
                if (digitsAfterDecimal == 0) {
                    return (int)genenratedNumber;
                }
                else {
                    logger.debug("rounding...");
                    return CommonHelper.round(genenratedNumber, digitsAfterDecimal);
                }
            }
            return genenratedNumber;
        } else {
            logger.debug("number does not depend on previous row");
            if(isNumberRangeBased(format, row)) {
                logger.debug("number is range based");
                double rangeStart = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row));
                double rangeEnd = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeEnd",format, row));
                double genenratedNumber = generateRandomNumber(rangeStart, rangeEnd);
                logger.debug("genenratedNumber =>"+genenratedNumber);
                int digitsAfterDecimal = 0;
                Object digitsAfterDecimalObject = MetaDataHelper.getColumnFormatParameterValue("digitsAfterDecimal",format, row);            
                if (digitsAfterDecimalObject != null) {
                    digitsAfterDecimal = Integer.parseInt((String)digitsAfterDecimalObject);
                }
                logger.debug("digitsAfterDecimalObject => "+digitsAfterDecimalObject);
                if (digitsAfterDecimalObject != null) {
                    logger.debug("digitsAfterDecimal => "+digitsAfterDecimal);
                    if (digitsAfterDecimal == 0) {
                        return (int)genenratedNumber;
                    }
                    else {
                        logger.debug("rounding...");
                        return CommonHelper.round(genenratedNumber, digitsAfterDecimal);
                    }
                }

                return genenratedNumber;
            }
            if (isNumberSynthetic(format, row)) {
                logger.debug("number is synthetic");
                int digitsBeforeDecimal = Integer.parseInt((String)MetaDataHelper.getColumnFormatParameterValue("digitsBeforeDecimal",format, row));
                int digitsAfterDecimal = Integer.parseInt((String)MetaDataHelper.getColumnFormatParameterValue("digitsAfterDecimal",format, row));
                double genenratedNumber = generateRandomNumber(digitsBeforeDecimal, digitsAfterDecimal);
                logger.debug("genenratedNumber =>"+genenratedNumber);
                if (digitsAfterDecimal == 0) {
                    return (int)genenratedNumber; 
                }
                return genenratedNumber;
            }
        }
        throw new Exception("don't know how to generate number for column ("+columnName+")");
    }

    private static double generateNumberWithSpacing(double originalNumber, double spacing, 
        boolean randomSpacing, String increment, double rangeStart, double rangeEnd) throws Exception {

        logger.debug("originalNumber => "+originalNumber);
        logger.debug("spacing => "+spacing);
        logger.debug("randomSpacing => "+randomSpacing);
        logger.debug("increment => "+increment);
        logger.debug("rangeStart => "+rangeStart);
        logger.debug("rangeEnd => "+rangeEnd);

        double option1 = 0;
        double option2 = 0;
        if (randomSpacing) {
            option1 = ThreadLocalRandom.current().nextDouble(originalNumber, originalNumber+spacing);
            option2 = ThreadLocalRandom.current().nextDouble(originalNumber-spacing, originalNumber);
        }
        else {
            option1 = originalNumber + spacing;
            option2 = originalNumber - spacing;
        }
        
        logger.debug("option1 => "+option1);
        logger.debug("option2 => "+option2);

        double toReturn = option1;
        boolean shouldIncrement = false;
        boolean ignore = true;

        if (increment != null) {
            logger.debug("increment not null ("+increment+")");
            if (increment.trim().equals("true")) {
                toReturn = option1;
                shouldIncrement = true;
            }
            else {
                toReturn = option2;
                shouldIncrement = false;
            }
            ignore = false;
        } 

        if (rangeStart != -1.09090 && rangeEnd != -1.09090) {
            if (!ignore && shouldIncrement) {
                if (option1 >= rangeStart && option1 <= rangeEnd) {
                    toReturn = option1;
                }
            } else if (!ignore && !shouldIncrement) {
                if (option2 >= rangeStart && option2 <= rangeEnd) {
                    toReturn = option2;
                }
            } else {
                if (option1 >= rangeStart && option1 <= rangeEnd) {
                    toReturn = option1;
                } else if (option2 >= rangeStart && option2 <= rangeEnd) {
                    toReturn = option2;
                }
            }
            
        }
        logger.debug("toReturn => "+toReturn);
        return toReturn;
    }

    private static double generateRandomNumber(int digitsBeforeDecimal, int digitsAfterDecimal) throws Exception {
        long beforeDecimal = ThreadLocalRandom.current().nextLong(getMinLongOfGivenDigits(digitsBeforeDecimal), getMaxLongOfGivenDigits(digitsBeforeDecimal));
        long afterDecimal = ThreadLocalRandom.current().nextLong(getMinLongOfGivenDigits(digitsAfterDecimal), getMaxLongOfGivenDigits(digitsAfterDecimal));
        String number = ""+((digitsBeforeDecimal > 0)?beforeDecimal:"0")+"."+((digitsAfterDecimal > 0)?afterDecimal:"");
        return Double.parseDouble(number);
    }

    private static double generateRandomNumber(double rangeStart, double rangeEnd) throws Exception {
        double number = ThreadLocalRandom.current().nextDouble(rangeStart, rangeEnd);
        return number;
    }

    private static long getMinLongOfGivenDigits(int digits) throws Exception {
        String startWith = "1";
        for (int i = 0 ; i < (digits-1) ; i++) {
            startWith += "0";
        }
        return Long.parseLong(startWith);
    }

    private static long getMaxLongOfGivenDigits(int digits) throws Exception {
        String startWith = "9";
        for (int i = 0 ; i < (digits-1) ; i++) {
            startWith += "9";
        }
        return Long.parseLong(startWith);
    }

    /// date data generation start
    private static Logger dateGenlogger = LogManager.getLogger("dateGenlogger");

    private static boolean doesDateDependOnPreviousRow(HashMap format, HashMap row) throws Exception {
        Object spacing = MetaDataHelper.getColumnFormatParameterValue("spacingFromPreviousRow",format, row);
        if (spacing == null) {
            return false;
        } else {
            return true;
        }
    }

    private static Date generateDateWithSpacing(Date originalDate, int spacing, String spacingType,
    boolean randomSpacing, String increment, Date rangeStart, Date rangeEnd) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

        logger.debug("originalDate => "+originalDate);
        logger.debug("spacing => "+spacing);
        logger.debug("randomSpacing => "+randomSpacing);
        logger.debug("spacingType => "+spacingType);
        logger.debug("increment => "+increment);
        logger.debug("rangeStart => "+rangeStart);
        logger.debug("rangeEnd => "+rangeEnd);

        Date option1 = null;
        Date option2 = null;
        if (randomSpacing) {
            int randomSpace = ThreadLocalRandom.current().nextInt(0, spacing);
            option1 = CommonHelper.addDurationToDate(originalDate, randomSpace, spacingType);
            option2 = CommonHelper.addDurationToDate(originalDate, -randomSpace, spacingType);
        }
        else {
            option1 = CommonHelper.addDurationToDate(originalDate, spacing, spacingType);
            option2 = CommonHelper.addDurationToDate(originalDate, -spacing, spacingType);
        }
        
        logger.debug("option1 => "+option1);
        logger.debug("option2 => "+option2);

        Date toReturn = option1;
        boolean shouldIncrement = false;
        boolean ignore = true;

        if (increment != null) {
            logger.debug("increment not null ("+increment+")");
            if (increment.trim().equals("true")) {
                toReturn = option1;
                shouldIncrement = true;
            }
            else {
                toReturn = option2;
                shouldIncrement = false;
            }
            ignore = false;
        } 

        if (!rangeStart.equals(sdf.parse("01-01-1770")) && !rangeEnd.equals(sdf.parse("01-01-1770"))) {
            if (!ignore && shouldIncrement) {
                if (option1.compareTo(rangeStart) >= 0 && option1.compareTo(rangeEnd) <= 0) {
                    toReturn = option1;
                }
            } else if (!ignore && !shouldIncrement) {
                if (option2.compareTo(rangeStart) >= 0 && option2.compareTo(rangeEnd) <= 0) {
                    toReturn = option2;
                }
            } else {
                if (option1.compareTo(rangeStart) >= 0 && option1.compareTo(rangeEnd) <= 0) {
                    toReturn = option1;
                } else if (option2.compareTo(rangeStart) >= 0 && option2.compareTo(rangeEnd) <= 0) {
                    toReturn = option2;
                }
            }
            
        }
        logger.debug("toReturn => "+toReturn);
        return toReturn;
    }

    private static Date generateRandomDate(Date rangeStart, Date rangeEnd) throws Exception {
        dateGenlogger.debug("\n\n");
        dateGenlogger.debug("rangeStart => "+rangeStart);
        dateGenlogger.debug("rangeEnd => "+rangeEnd);

        if (rangeStart == null) {
            throw new Exception("rangeStart date cannot be null");
        }
        if (rangeEnd == null) {
            throw new Exception("rangeEnd date cannot be null");
        }
        if (rangeStart.compareTo(rangeEnd) > 0) {
            throw new Exception("rangeStart ("+rangeStart+") cannot be greater than range end ("+rangeEnd+")");
        }

        int daysDiff = CommonHelper.daysBetween(rangeStart, rangeEnd);
        dateGenlogger.debug("daysDiff => "+daysDiff);

        int randomDays = ThreadLocalRandom.current().nextInt(0, daysDiff);
        dateGenlogger.debug("daysDiff => "+daysDiff);

        Date randomDate = CommonHelper.addDurationToDate(rangeStart, randomDays, "days");
        dateGenlogger.debug("randomDate => "+randomDate);

        return randomDate;
    }

    public static Date generateDate(String columnName, HashMap format, HashMap row, HashMap previousRow) throws Exception {
        
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

        Date rangeStart = sdf.parse("01-01-1770");
        Object rangeStartObj = MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row);
        dateGenlogger.debug("rangeStartObj => "+rangeStartObj);
        if (rangeStartObj != null) {
            rangeStart = sdf.parse((String)rangeStartObj);
        }
        dateGenlogger.debug("rangeStart => "+rangeStart);

        Date rangeEnd = sdf.parse("01-01-1770");
        Object rangeEndObj = MetaDataHelper.getColumnFormatParameterValue("rangeEnd",format, row);
        dateGenlogger.debug("rangeEndObj => "+rangeEndObj);
        if (rangeEndObj != null) {
            rangeEnd = sdf.parse((String)rangeEndObj);
        }
        dateGenlogger.debug("rangeEnd => "+rangeEnd);


        if (doesDateDependOnPreviousRow(format, row) && previousRow != null) {
            logger.debug("date depends on previous row");
            Date date = (Date)previousRow.get(columnName);
            if (date == null) {
                throw new Exception("date doesnt exist for previous row");
            }

            Object spacingStr = MetaDataHelper.getColumnFormatParameterValue("spacingFromPreviousRow",format, row);
            int spacing = 4;
            if (spacingStr != null) {
                spacing = Integer.parseInt((String)spacingStr);
            }

            Object randomSpacingStr = MetaDataHelper.getColumnFormatParameterValue("randomSpacing",format, row);
            boolean randomSpacing = false;
            if (randomSpacingStr != null) {
                randomSpacing = Boolean.parseBoolean((String)randomSpacingStr);
            }
            
            Object incrementStr = MetaDataHelper.getColumnFormatParameterValue("applySpacingWithIncrement",format, row);
            String increment = null;
            if (incrementStr != null) {
                increment = incrementStr.toString();
            }
            

            Object spacingType = MetaDataHelper.getColumnFormatParameterValue("spacingType",format, row);
            if (spacingType == null) {
                spacingType = "days";
            }

            Date genenratedDate = generateDateWithSpacing(date, spacing, (String)spacingType, randomSpacing, increment, rangeStart, rangeEnd);
            
            logger.debug("genenratedDate =>"+genenratedDate);
            
            return genenratedDate;
        } else {
            logger.debug("date does not depend on previous row");
            
            Date genenratedDate = generateRandomDate(rangeStart, rangeEnd);
            logger.debug("genenratedNumber =>"+genenratedDate);
            

            return genenratedDate;
        
        }
    }

    /// date data generation end

    private static Logger enrichLogger = LogManager.getLogger("enrichLogger");

    public static ArrayList<HashMap> filterRows(DataSet dataSet, HashMap criteria) throws Exception {
        ArrayList<HashMap> toReturn = new ArrayList();
        if (criteria == null || criteria.keySet().size() == 0) {
            return dataSet.getGeneratedRows();
        }
        ArrayList<HashMap> rows = dataSet.getGeneratedRows();
        for (HashMap row : rows) {
            boolean criteriaMatch = true;
            for (Object key : criteria.keySet()) {
                Object criteriaValue = criteria.get(key);
                Object rowValue = row.get(key);
                if (criteriaValue != null && rowValue != null) {
                    if (!criteriaValue.equals(rowValue)) {
                        criteriaMatch = false;
                        break;
                    }
                }
            }
            if (criteriaMatch) {
                toReturn.add(row);
            }
        }
        return toReturn;
    }

    public static HashMap selectRandomRow(DataSet dataSet) throws Exception {
        int randomIndex = ThreadLocalRandom.current().nextInt(0, dataSet.getGeneratedRows().size());
        return dataSet.getGeneratedRows().get(randomIndex);
    }

    public static HashMap selectRandomRow(ArrayList<HashMap> rows) throws Exception {
        int randomIndex = ThreadLocalRandom.current().nextInt(0, rows.size());
        return rows.get(randomIndex);
    }

    private static ArrayList<HashMap> getRowParents(HashMap<String, Object> row) throws Exception {
        ArrayList<HashMap> toReturn = new ArrayList();
        for (String key : row.keySet()) {
            if (key.trim().startsWith("@parent")) {
                toReturn.add((HashMap)row.get(key));
            }
        }
        return toReturn;
    }

    private static ArrayList<DataSet> getRowParentDataSets(DataSet dataSetUnderProcessing, HashMap<String, Object> row) throws Exception {
        ArrayList<DataSet> toReturn = new ArrayList();
        for (String key : row.keySet()) {
            if (key.trim().startsWith("@parent")) {
                String parentDataSet = (String)((HashMap)row.get(key)).get("@dataset");
                toReturn.add(dataSetUnderProcessing.getMetaReader().getDataSet(parentDataSet));
            }
        }
        return toReturn;
    }

    public static ArrayList<DataSet> getMissingParents(DataSet dataSetUnderProcess, HashMap row) throws Exception {
        ArrayList<DataSet> missingDataSets = new ArrayList();
        ArrayList<String> parentsInRow = (ArrayList<String>)row.keySet()
                     .stream()
                     .filter(s -> s.toString().startsWith("@parent"))
                     .map(x -> x.toString().substring(x.toString().indexOf("-")+1))
                     .collect(Collectors.toList());

        enrichLogger.debug("parentsInRow => "+parentsInRow);
        
        ArrayList<DataSet> parentRelatedSets = dataSetUnderProcess.getRelatedDataSetsOneSide();

        for (DataSet parentSet : parentRelatedSets) {
            if (!parentsInRow.contains(parentSet.getName())) {
                missingDataSets.add(parentSet);
            }
        }

        enrichLogger.debug("missingDataSets => "+missingDataSets);
        return missingDataSets;
    }

    public static ArrayList<HashMap> selectRowsMatchingWithPossiblyRelatedRows(DataSet dataSet, ArrayList<HashMap> possiblyRelatedRows) throws Exception {
        HashMap criteria = new HashMap();
        for (HashMap possiblyRelatedRow : possiblyRelatedRows) {
            String possiblyRelatedDataSet = (String)possiblyRelatedRow.get("@dataset");
            DataSet relatedDataSet = dataSet.getRelatedDataSetByName(possiblyRelatedDataSet);
            if (relatedDataSet != null) {
                String relationShipType = dataSet.getRelationshipType(relatedDataSet);
                String dataSetSideColumn = dataSet.getSpecificRelationParameter(relatedDataSet, "thisColumn");
                String relatedColumn = dataSet.getSpecificRelationParameter(relatedDataSet, "relatedColumn");
                criteria.put(dataSetSideColumn, possiblyRelatedRow.get(relatedColumn));
            }
        }
        return filterRows(dataSet, criteria);
    }

    public static HashMap enrichForeignKeyValuesByAddingMissingValuesFromAllParents(DataSet dataSetUnderProcess, ArrayList<String> dataSetsAlreadyGeneratedRowsFor, HashMap incomingForeignKeyValues, HashMap rowUnderConstruction) throws Exception {
        enrichLogger.debug("=========== enrich foreign key ==========");
        enrichLogger.debug("dataSetUnderProcess => "+dataSetUnderProcess);
        enrichLogger.debug("incomingForeignKeyValues => "+incomingForeignKeyValues);
        enrichLogger.debug("rowUnderConstruction => "+rowUnderConstruction);
        enrichLogger.debug("dataSetsAlreadyGeneratedRowsFor => "+dataSetsAlreadyGeneratedRowsFor);

        ArrayList<HashMap> parentRowsForTheGivenRow = getRowParents(rowUnderConstruction);
        enrichLogger.debug("parentRowsForTheGivenRow => "+parentRowsForTheGivenRow);

        ArrayList<DataSet> missingParents = getMissingParents(dataSetUnderProcess, rowUnderConstruction);
        enrichLogger.debug("missingParents => "+missingParents);

        for (DataSet missingParent : missingParents) {
            if (!dataSetsAlreadyGeneratedRowsFor.contains(missingParent.getName())) {
                enrichLogger.debug("missing parent ("+missingParent.getName()+") is not yet generated... generating now");
                
                // first generate missing parent before moving forward
                ArrayList<String> avoidedDataSets = new ArrayList();
                avoidedDataSets.add(dataSetUnderProcess.getName());
                ArrayList<DataSet> parentDataSets = getRowParentDataSets(dataSetUnderProcess, rowUnderConstruction);
                for (DataSet parentDataSet : parentDataSets) {
                    avoidedDataSets.add(parentDataSet.getName());
                }
                enrichLogger.debug("avoidedDataSets => "+avoidedDataSets);
                ArrayList<String> generatedDataSets = missingParent.generateRows(avoidedDataSets);
                
                dataSetsAlreadyGeneratedRowsFor.add(missingParent.getName());
                dataSetsAlreadyGeneratedRowsFor.add(missingParent.getName()+"-fully-done");
                for (String generatedDataSetName : generatedDataSets) {
                    if (!dataSetsAlreadyGeneratedRowsFor.contains(generatedDataSetName)) {
                        dataSetsAlreadyGeneratedRowsFor.add(generatedDataSetName);
                    }
                }
                enrichLogger.debug("missing parent ("+missingParent.getName()+") generated fully");
            }
            ArrayList<HashMap> matchingRows = selectRowsMatchingWithPossiblyRelatedRows(missingParent, parentRowsForTheGivenRow);
            enrichLogger.debug("matchingRows => "+matchingRows);

            HashMap randomRow = selectRandomRow(matchingRows);
            enrichLogger.debug("randomRow => "+randomRow);

            rowUnderConstruction.put("@parent-"+missingParent.getName(), randomRow);
            parentRowsForTheGivenRow = getRowParents(rowUnderConstruction);

            enrichLogger.debug("parentRowsForTheGivenRow 1 => "+parentRowsForTheGivenRow);

            String dataSetSideColumn = dataSetUnderProcess.getSpecificRelationParameter(missingParent, "thisColumn");
            String relatedColumn = dataSetUnderProcess.getSpecificRelationParameter(missingParent, "relatedColumn");

            incomingForeignKeyValues.put(dataSetSideColumn, randomRow.get(relatedColumn));
        }
        enrichLogger.debug("enriched incomingForeignKeyValues => "+incomingForeignKeyValues);
        return incomingForeignKeyValues;
    }
}