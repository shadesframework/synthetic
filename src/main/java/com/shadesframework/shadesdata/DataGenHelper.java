package com.shadesframework.shadesdata;
import com.mifmif.common.regex.Generex;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataGenHelper {
    private static Logger logger = LogManager.getLogger(DataGenHelper.class);

    public static String generateString(DataSet dataSet, ArrayList<String> dataSetsAlreadyGeneratedRowsFor, String columnName, HashMap format, HashMap row, HashMap previousRow) throws Exception {
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
            generatedString = pickNextStringFromList(dataSet, dataSetsAlreadyGeneratedRowsFor, (ArrayList)rotate, columnName, previousRow);
            return generatedString;
        }

        throw new Exception("don't know how to generate string for column ("+dataSet+"."+columnName+")");
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

    private static Logger rotateLogger = LogManager.getLogger("rotateLogger");
    private static String pickNextStringFromList(DataSet dataSet, ArrayList<String> dataSetsAlreadyGeneratedRowsFor, ArrayList rotateList, String columnName, HashMap previousRow) throws Exception {
        rotateLogger.debug("\n\n\n===pickNextStringFromList===");
        rotateLogger.debug("dataSet ("+dataSet+")");
        rotateLogger.debug("dataSetsAlreadyGeneratedRowsFor ("+dataSetsAlreadyGeneratedRowsFor+")");
        rotateLogger.debug("rotateList ("+rotateList+")");
        rotateLogger.debug("columnName ("+columnName+")");
        rotateLogger.debug("previousRow ("+previousRow+")");

        if (previousRow == null) {
            String entryValue = evaluateEntry(dataSet, dataSetsAlreadyGeneratedRowsFor, columnName, rotateList.get(0), previousRow);
            rotateLogger.debug("no prev row entry value ("+entryValue+")");
            return entryValue;
        }
        
        rotateLogger.debug("previousRowStringObj ("+previousRow.get(columnName)+")");
        Object previousRowString = previousRow.get(columnName);
        rotateLogger.debug("previousRowString ("+previousRowString+")");
        
        int indexToPick = 0;
        if (previousRowString != null) {
            indexToPick = findStringinListOfEntries(rotateList, previousRowString, dataSet, columnName, previousRow, dataSetsAlreadyGeneratedRowsFor) + 1;
            rotateLogger.debug("indexToPick ("+indexToPick+")");
            if (indexToPick < 0) {
                indexToPick = 0;
            }
            if (indexToPick > (rotateList.size()-1)) {
                indexToPick = 0;
            }
        }
        String entryValue = evaluateEntry(dataSet, dataSetsAlreadyGeneratedRowsFor, columnName, rotateList.get(indexToPick), previousRow);
        rotateLogger.debug("entry value for index ("+indexToPick+") ("+entryValue+")");
        rotateLogger.debug("\n\n\n===end-pickNextStringFromList===");
        return entryValue;
    }

    private static int findStringinListOfEntries(ArrayList rotateList, Object stringToFind, DataSet dataSet, String columnName, HashMap previousRow, ArrayList<String> dataSetsAlreadyGeneratedRowsFor) throws Exception {
        int i = 0;
        for (Object entry : rotateList) {
            String entryValue = evaluateEntry(dataSet, dataSetsAlreadyGeneratedRowsFor, columnName, entry, previousRow);
            if (stringToFind != null 
                    && entryValue.trim().equals(stringToFind.toString())) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private static String evaluateEntry(DataSet dataSet, ArrayList<String> dataSetsAlreadyGeneratedRowsFor, String columnName, Object entry, HashMap previousRow) throws Exception {
        if (entry instanceof String) {
            return (String)entry;
        } else if (Number.class.isAssignableFrom(entry.getClass())) {
            return entry.toString();
        } else if (entry instanceof HashMap) {
            return evaluateGrabber(dataSet, dataSetsAlreadyGeneratedRowsFor, columnName, (HashMap)entry, previousRow);
        }
        else {
            throw new Exception("dont know how to process entry ("+entry+")");
        }
    }

    private static String evaluateGrabber(DataSet dataSet, ArrayList<String> dataSetsAlreadyGeneratedRowsFor, String columnName, HashMap grabber, HashMap previousRow) throws Exception {
        rotateLogger.debug("dataSet ("+dataSet+")");
        rotateLogger.debug("dataSetsAlreadyGeneratedRowsFor ("+dataSetsAlreadyGeneratedRowsFor+")");
        rotateLogger.debug("columnName => "+ columnName);
        rotateLogger.debug("grabber => "+ grabber);
        rotateLogger.debug("previousRow => "+ previousRow);

        String keyToReturn = (String)grabber.get("valuekey");
        rotateLogger.debug("keyToReturn => "+keyToReturn);
        if (keyToReturn == null ||
            keyToReturn.trim().equals("")) {
            throw new Exception("valuekey cannot be null in ("+grabber+")");
        }
        HashMap<String, Object> selector = (HashMap)grabber.get("selector");
        rotateLogger.debug("selector => "+selector);
        if (selector == null) {
            throw new Exception("selector cannot be null in ("+grabber+")");
        }
        String dataSetStr = (String)selector.get("dataset");
        rotateLogger.debug("dataSetStr =>"+dataSetStr);
        if (dataSetStr == null ||
            dataSetStr.trim().equals("")) {
            throw new Exception("dataset cannot be null in ("+grabber+")");
        }
        dataSetStr = dataSetStr.trim();
        HashMap query = (HashMap)selector.get("query");
        rotateLogger.debug("query =>"+query);
        if (query == null) {
            throw new Exception("query cannot be null in ("+grabber+")");
        }
        if (dataSetStr.trim().startsWith("@related")) {
            ArrayList<DataSet> relatedDataSets = dataSet.getRelatedDataSetsByThisColumnName(columnName);
            if (relatedDataSets.size() == 0) {
                throw new Exception("column ("+dataSet.getName()+"."+columnName+") is not related to any other dataset. specify the dataset");
            }
            if (relatedDataSets.size() > 1) {
                dataSetStr = dataSetStr.substring(dataSetStr.indexOf("@related.")+9).trim();
            }
            if (relatedDataSets.size() == 1) {
                dataSetStr = relatedDataSets.get(0).getName().trim();
            }
        }

        DataSet dataSetToQuery = dataSet.getMetaReader().getDataSet(dataSetStr);
        rotateLogger.debug("dataSetToQuery =>"+dataSetToQuery);
        if (dataSetToQuery == null) {
            throw new Exception("grabber data set ("+dataSetToQuery+") not found while generating column ("+dataSet.getName()+"."+columnName+")");
        }
        if (dataSetToQuery.getGeneratedRows() == null ||
                dataSetToQuery.getGeneratedRows().size() == 0) {
                rotateLogger.debug("data set to query/grabber ("+dataSetToQuery.getName()+") is not yet generated... generating now");
            
                // first generate missing parent before moving forward
                ArrayList<String> avoidedDataSets = new ArrayList();
                avoidedDataSets.add(dataSet.getName());
                ArrayList<DataSet> parentDataSets = getRowParentDataSets(dataSet, previousRow);
                for (DataSet parentDataSet : parentDataSets) {
                    avoidedDataSets.add(parentDataSet.getName());
                }
                rotateLogger.debug("avoidedDataSets => "+avoidedDataSets);
                ArrayList<String> generatedDataSets = dataSetToQuery.generateRows(avoidedDataSets);
                
                dataSetsAlreadyGeneratedRowsFor.add(dataSetToQuery.getName());
                dataSetsAlreadyGeneratedRowsFor.add(dataSetToQuery.getName()+"-fully-done");
                for (String generatedDataSetName : generatedDataSets) {
                    if (!dataSetsAlreadyGeneratedRowsFor.contains(generatedDataSetName)) {
                        dataSetsAlreadyGeneratedRowsFor.add(generatedDataSetName);
                    }
                }
                rotateLogger.debug("data set to query ("+dataSetToQuery.getName()+") generated fully");

        }
        rotateLogger.debug("dataSetToQuery.rows => "+dataSetToQuery.getGeneratedRows());
        ArrayList<HashMap> queryResult = filterRows(dataSetToQuery, query);
        rotateLogger.debug("queryResult => "+queryResult);
        if (queryResult.size() > 0) {
            int randomIndex = ThreadLocalRandom.current().nextInt(0, queryResult.size());
            return queryResult.get(randomIndex).get(keyToReturn).toString();
        }
        return null;
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

    private static boolean isNumberRandomPick(HashMap format, HashMap row) throws Exception {
        Object randomPick = MetaDataHelper.getColumnFormatParameterValue("randomPick",format, row);
        if (randomPick == null) {
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

    private static boolean isNumberRotational(HashMap format, HashMap row) throws Exception {
        Object rotate = MetaDataHelper.getColumnFormatParameterValue("rotate",format, row);
        if (rotate == null) {
            return false;
        } else {
            return true;
        }
    }
    
    private static Logger numberLogger = LogManager.getLogger("numberLogger");

    public static Number generateNumber(DataSet dataSet, ArrayList<String> dataSetsAlreadyGeneratedRowsFor, String columnName, HashMap format, HashMap row, HashMap previousRow) throws Exception {
        numberLogger.debug("columnName ("+columnName+")");
        numberLogger.debug("format ("+format+")");
        numberLogger.debug("row ("+row+")");
        numberLogger.debug("previousRow ("+previousRow+")");

        if (doesNumberDependOnPreviousRow(format, row) && previousRow != null) {
            numberLogger.debug("number depends on previous row");
            Number number = (Number)previousRow.get(columnName);
            if (number == null) {
                throw new Exception("number doesnt exist for previous row");
            }
            numberLogger.debug("number ("+number+")");

            double spacing = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("spacingFromPreviousRow",format, row));
            numberLogger.debug("spacing ("+spacing+")");

            boolean randomSpacing = Boolean.parseBoolean((String)MetaDataHelper.getColumnFormatParameterValue("randomSpacing",format, row));
            numberLogger.debug("randomSpacing ("+randomSpacing+")");

            String increment = (String)MetaDataHelper.getColumnFormatParameterValue("applySpacingWithIncrement",format, row);
            numberLogger.debug("increment ("+increment+")");
            
            double rangeStart = -1.09090;
            Object rangeStartObj = MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row);
            if (rangeStartObj != null) {
                rangeStart = Double.parseDouble((String)rangeStartObj);
            }
            numberLogger.debug("rangeStart ("+rangeStart+")");

            double rangeEnd = -1.09090;
            Object rangeEndObj = MetaDataHelper.getColumnFormatParameterValue("rangeEnd",format, row);
            if (rangeEndObj != null) {
                rangeEnd = Double.parseDouble((String)rangeEndObj);
            }
            numberLogger.debug("rangeEnd ("+rangeEnd+")");

            int digitsAfterDecimal = 0;
            Object digitsAfterDecimalObject = MetaDataHelper.getColumnFormatParameterValue("digitsAfterDecimal",format, row);            
            if (digitsAfterDecimalObject != null) {
                digitsAfterDecimal = Integer.parseInt((String)digitsAfterDecimalObject);
            }
            numberLogger.debug("digitsAfterDecimal ("+digitsAfterDecimal+")");

            double genenratedNumber = generateNumberWithSpacing(number.doubleValue(), spacing, randomSpacing, increment, rangeStart, rangeEnd);
            
            numberLogger.debug("genenratedNumber =>"+genenratedNumber);
            numberLogger.debug("digitsAfterDecimalObject => "+digitsAfterDecimalObject);

            if (digitsAfterDecimalObject != null) {
                numberLogger.debug("digitsAfterDecimal => "+digitsAfterDecimal);
                if (digitsAfterDecimal == 0) {
                    return (int)genenratedNumber;
                }
                else {
                    numberLogger.debug("rounding...");
                    return CommonHelper.round(genenratedNumber, digitsAfterDecimal);
                }
            }
            return genenratedNumber;
        } else {
            numberLogger.debug("number does not depend on previous row");
            if(isNumberRangeBased(format, row)) {
                numberLogger.debug("number is range based");

                double rangeStart = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row));
                numberLogger.debug("rangeStart ("+rangeStart+")");

                double rangeEnd = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeEnd",format, row));
                numberLogger.debug("rangeEnd ("+rangeEnd+")");

                double genenratedNumber = generateRandomNumber(rangeStart, rangeEnd);
                numberLogger.debug("genenratedNumber =>"+genenratedNumber);

                int digitsAfterDecimal = 0;
                Object digitsAfterDecimalObject = MetaDataHelper.getColumnFormatParameterValue("digitsAfterDecimal",format, row);            
                if (digitsAfterDecimalObject != null) {
                    digitsAfterDecimal = Integer.parseInt((String)digitsAfterDecimalObject);
                }
                numberLogger.debug("digitsAfterDecimalObject => "+digitsAfterDecimalObject);
                if (digitsAfterDecimalObject != null) {
                    numberLogger.debug("digitsAfterDecimal => "+digitsAfterDecimal);
                    if (digitsAfterDecimal == 0) {
                        return (int)genenratedNumber;
                    }
                    else {
                        numberLogger.debug("rounding...");
                        return CommonHelper.round(genenratedNumber, digitsAfterDecimal);
                    }
                }

                return genenratedNumber;
            }
            if (isNumberSynthetic(format, row)) {
                numberLogger.debug("number is synthetic");
                int digitsBeforeDecimal = Integer.parseInt((String)MetaDataHelper.getColumnFormatParameterValue("digitsBeforeDecimal",format, row));
                int digitsAfterDecimal = Integer.parseInt((String)MetaDataHelper.getColumnFormatParameterValue("digitsAfterDecimal",format, row));
                double genenratedNumber = generateRandomNumber(digitsBeforeDecimal, digitsAfterDecimal);
                numberLogger.debug("genenratedNumber =>"+genenratedNumber);
                if (digitsAfterDecimal == 0) {
                    return (int)genenratedNumber; 
                }
                return genenratedNumber;
            }
            if (isNumberRandomPick(format, row)) {
                Object randomPick = MetaDataHelper.getColumnFormatParameterValue("randomPick",format, row);
                return pickRandomNumberFromList((ArrayList<Number>)randomPick);
            }
            if (isNumberRotational(format, row)) {
                Object rotateList = MetaDataHelper.getColumnFormatParameterValue("rotate",format, row);
                Object numberString = pickNextStringFromList(dataSet, dataSetsAlreadyGeneratedRowsFor, (ArrayList)rotateList, columnName, previousRow);
                return NumberFormat.getInstance().parse(numberString.toString()); 
            }
        }
        throw new Exception("don't know how to generate number for column ("+columnName+")");
    }

    private static Number pickRandomNumberFromList(ArrayList<Number> numberList) throws Exception {
        int randomIndex = ThreadLocalRandom.current().nextInt(0, numberList.size());
        return numberList.get(randomIndex);
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
                    Pattern p = Pattern.compile((String)criteriaValue.toString());
                    Matcher m = p.matcher((String)rowValue.toString());
                    if (!m.matches()) {
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

    public static HashMap enrichForeignKeyValuesByAddingMissingValuesFromAllParents(DataSet dataSetUnderProcess, ArrayList<String> dataSetsAlreadyGeneratedRowsFor, HashMap incomingForeignKeyValues, HashMap rowUnderConstruction, boolean dontGenerateMissingParents) throws Exception {
        enrichLogger.debug("\n\n\n=========== enrich foreign key ==========");
        enrichLogger.debug("dataSetUnderProcess => "+dataSetUnderProcess);
        enrichLogger.debug("incomingForeignKeyValues => "+incomingForeignKeyValues);
        enrichLogger.debug("rowUnderConstruction => "+rowUnderConstruction);
        enrichLogger.debug("dataSetsAlreadyGeneratedRowsFor => "+dataSetsAlreadyGeneratedRowsFor);
        enrichLogger.debug("dontGenerateMissingParents => "+dontGenerateMissingParents);

        if (incomingForeignKeyValues == null) {
            incomingForeignKeyValues = new HashMap();
        }

        if (dataSetsAlreadyGeneratedRowsFor == null) {
            dataSetsAlreadyGeneratedRowsFor = new ArrayList();
        }

        ArrayList<HashMap> parentRowsForTheGivenRow = getRowParents(rowUnderConstruction);
        enrichLogger.debug("parentRowsForTheGivenRow => "+parentRowsForTheGivenRow);

        ArrayList<DataSet> missingParents = getMissingParents(dataSetUnderProcess, rowUnderConstruction);
        enrichLogger.debug("missingParents => "+missingParents);

        for (DataSet missingParent : missingParents) {
            boolean generateMissingParentAgain = false;
            if (missingParent.getGeneratedRows() == null 
                    || missingParent.getGeneratedRows().size() == 0) {
                if (!dontGenerateMissingParents) {
                    generateMissingParentAgain = true;
                }
            }
            if (!dataSetsAlreadyGeneratedRowsFor.contains(missingParent.getName()) && generateMissingParentAgain) {
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
            //enrichLogger.debug("matchingRows => "+matchingRows);

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

    private static Logger repeatForColumnsLogger = LogManager.getLogger("repeatForColumnsLogger");
    
    public static void processRepeatForColumns(DataSet dataSet) throws Exception {
        repeatForColumnsLogger.debug("\n\n\n=====processRepeatForColumns====");
        repeatForColumnsLogger.debug("dataSet ("+dataSet+")");
        HashMap<String, ArrayList> repeatedCombinations = new HashMap();
        for (String columnName : dataSet.getColumns()) {
            HashMap format = dataSet.getDataFormat(columnName);
            Object repeatFor = MetaDataHelper.getColumnFormatParameterValue("repeatFor",format, null);
            if (repeatFor != null) {
                repeatedCombinations.put(columnName, (ArrayList)repeatFor);
            }
        }
        ArrayList<String> repeatedColumnNames = new ArrayList(repeatedCombinations.keySet());
        repeatForColumnsLogger.debug("repeatedCombinations ("+repeatedCombinations+")");

        ArrayList<UniqueColumnValuesTuple> uniqueValueTuples = new ArrayList();
        ArrayList<String> combinationColumnNames = new ArrayList();

        repeatForColumnsLogger.debug("dataSet.getUniqueValuesPerColumn() ("+dataSet.getUniqueValuesPerColumn()+")");

        for (String columnName : repeatedCombinations.keySet()) {
            ArrayList<String> repeatCombination = repeatedCombinations.get(columnName);
            for (String combinationColumn : repeatCombination) {
                HashSet uniqueValuesForColumn = dataSet.getUniqueValuesPerColumn().get(combinationColumn);
                repeatForColumnsLogger.debug("combinationColumn ("+combinationColumn+") uniqueValuesForColumn ("+uniqueValuesForColumn+")");
                UniqueColumnValuesTuple tuple = new UniqueColumnValuesTuple(combinationColumn, uniqueValuesForColumn);
                uniqueValueTuples.add(tuple);
                combinationColumnNames.add(combinationColumn);
            }
        }

        repeatForColumnsLogger.debug("uniqueValueTuples ("+uniqueValueTuples+")");
        repeatForColumnsLogger.debug("combinationColumnNames ("+combinationColumnNames+")");

        HashSet<HashMap> uniqueCombinations = createCombinations(uniqueValueTuples, combinationColumnNames, null, null);

        repeatForColumnsLogger.debug("uniqueCombinations ("+uniqueCombinations+")");

        ArrayList<HashMap> repeatedRows = new ArrayList();
        
        if (uniqueCombinations.size() > 0) {
            for (HashMap<String, Object> row : dataSet.getGeneratedRows()) {
                repeatForColumnsLogger.debug("row ("+row+")");

                for (String columnName : row.keySet()) {

                    repeatForColumnsLogger.debug("columnName ("+columnName+")");
                    repeatForColumnsLogger.debug("combinationColumnNames.contains(columnName) ("+combinationColumnNames.contains(columnName)+")");

                    if (combinationColumnNames.contains(columnName)) {
                        // this column needs to be created repeated rows for
                        ArrayList<HashMap> repeatedRowsForGivenRow = createRepeatRowsWithCombinations(row,uniqueCombinations, repeatedColumnNames, combinationColumnNames, dataSet);
                        repeatForColumnsLogger.debug("repeatedRowsForGivenRow ("+repeatedRowsForGivenRow+")");
                        repeatedRows.addAll(repeatedRowsForGivenRow);
                        break;
                    }
                }
            }

            repeatForColumnsLogger.debug("repeatedRows ("+repeatedRows+")");
            if (repeatedRows.size() > 0) {
                dataSet.getGeneratedRows().addAll(repeatedRows);
                dataSet.setRepeatedDataAdded(true);
            }
        }
        
    }

    private static ArrayList<HashMap> createRepeatRowsWithCombinations(HashMap baseRow, HashSet<HashMap> uniqueCombinations, ArrayList<String> repeatedColumnNames, ArrayList<String> combinationColumnNames, DataSet dataSet) throws Exception {
        repeatForColumnsLogger.debug("baseRow ("+baseRow+")");
        ArrayList<HashMap> toReturn = new ArrayList();
        for (HashMap<String, Object> uniqueCombination : uniqueCombinations) {

            repeatForColumnsLogger.debug("uniqueCombination ("+uniqueCombination+")");
            repeatForColumnsLogger.debug("isCombinationAlreadyPresent(baseRow, uniqueCombination) ("+isCombinationAlreadyPresent(baseRow, uniqueCombination)+")");

            if (!isCombinationAlreadyPresent(baseRow, uniqueCombination)) {
                HashMap cloneRow = (HashMap)baseRow.clone();
                for (String key : uniqueCombination.keySet()) {
                    cloneRow.put(key, uniqueCombination.get(key));
                }
                
                synchronizeParentsWithRow(dataSet, cloneRow);

                ArrayList<String> columnsNotToBeRegenerated = new ArrayList();
                columnsNotToBeRegenerated.addAll(combinationColumnNames);
                columnsNotToBeRegenerated.addAll(repeatedColumnNames);
                regenerateColumnsNotPartOfCombination(cloneRow, columnsNotToBeRegenerated, dataSet);
                toReturn.add(cloneRow);   
            }
        }
        return toReturn;
    }

    private static void removeParentRows(HashMap<String, Object> row) throws Exception {
        HashMap<String, Object> tempClone = (HashMap)row.clone();
        for (String key : tempClone.keySet()) {
            if(key.trim().startsWith("@parent")) {
                row.remove(key);
            }
        }
    }

    private static void synchronizeParentsWithRow(DataSet dataSet, HashMap<String, Object> row) throws Exception {
        removeParentRows(row);

        ArrayList<DataSet> missingParents = getMissingParents(dataSet, row);
        repeatForColumnsLogger.debug("missingParents => "+missingParents);

        for (DataSet missingParent : missingParents) {

            repeatForColumnsLogger.debug("missingParent => "+missingParent);

            String relatedColumnName = dataSet.getRelatedColumn(missingParent);
            String thisColumnName = dataSet.getThisColumn(missingParent);
            Object thisColumnValue = row.get(thisColumnName);

            repeatForColumnsLogger.debug("relatedColumnName => "+relatedColumnName);
            repeatForColumnsLogger.debug("thisColumnName => "+thisColumnName);
            repeatForColumnsLogger.debug("thisColumnValue => "+thisColumnValue);

            HashMap criteria = new HashMap();
            criteria.put(relatedColumnName, thisColumnValue);

            repeatForColumnsLogger.debug("criteria => "+criteria);

            ArrayList<HashMap> matchingRows = filterRows(missingParent, criteria);
            repeatForColumnsLogger.debug("matchingRows => "+matchingRows);

            HashMap randomRow = selectRandomRow(matchingRows);
            repeatForColumnsLogger.debug("randomRow => "+randomRow);

            row.put("@parent-"+missingParent.getName(), randomRow);
            
        }
    }

    private static HashMap regenerateColumnsNotPartOfCombination(HashMap<String, Object> row, ArrayList<String> columnsNotToBeRegenerated, DataSet dataSet) throws Exception {
        
        repeatForColumnsLogger.debug("\n\n==== regenerateColumnsNotPartOfCombination ====");
        repeatForColumnsLogger.debug("row ("+row+")");
        repeatForColumnsLogger.debug("columnsNotToBeRegenerated ("+columnsNotToBeRegenerated+")");

        for (String columnName : row.keySet()) {
            if (!columnsNotToBeRegenerated.contains(columnName) && !columnName.trim().startsWith("@")) {
                // re-generate this column
                
                repeatForColumnsLogger.debug("regenerating columnName ("+columnName+")");

                String dataType = dataSet.getDataType(columnName);
                HashMap format = dataSet.getDataFormat(columnName);
                
                repeatForColumnsLogger.debug("dataType ("+dataType+")");
                repeatForColumnsLogger.debug("format ("+format+")");

                if (format == null) {
                    throw new Exception("format cannot be null for column ("+dataSet.getName()+"."+columnName+")");
                }
                Object columnValue = null;
                HashMap previousRow =null;

                int tryGeneratingUniqueValueCount = 0;
                boolean isValueGeneratedCorrecly = false;
                if (dataType != null) {
                    while (tryGeneratingUniqueValueCount < 3) {

                        int uniqueIntegerForPrimaryKey = UUID.randomUUID().toString().hashCode();
                        repeatForColumnsLogger.debug("uniqueIntegerForPrimaryKey ("+uniqueIntegerForPrimaryKey+")");

                        if (dataType.toLowerCase().trim().equals("number")) {
                            if (dataSet.isColumnPrimaryKey(columnName)) {
                                columnValue = uniqueIntegerForPrimaryKey;
                            } else {
                                columnValue = DataGenHelper.generateNumber(dataSet, null, columnName,format, row, previousRow);
                            }
                        }
                        else if (dataType.toLowerCase().trim().equals("string")) {
                            if (dataSet.isColumnPrimaryKey(columnName)) {
                                columnValue = ""+uniqueIntegerForPrimaryKey;
                            } else {
                                columnValue = DataGenHelper.generateString(dataSet, null, columnName, format, row, previousRow);
                            }
                        } 
                        else if (dataType.toLowerCase().trim().equals("date")) {
                            columnValue = DataGenHelper.generateDate(columnName, format, row, previousRow);
                        } else {
                            throw new Exception("unrecognized datatype ("+dataType+") specified for column ("+dataSet.getName()+"."+columnName+")");
                        }
                        if (!dataSet.isColumnPrimaryKey(columnName)) {
                            isValueGeneratedCorrecly = true;
                            break;
                        } else {
                            // check if the value is not already generated for this column (being PK)
                            if (!dataSet.isValueAlreadyPresentInPrimaryKey(columnName, columnValue)) {
                                repeatForColumnsLogger.debug("value not present in primary key from before");
                                isValueGeneratedCorrecly = true;
                                break;
                            }
                            else {
                                repeatForColumnsLogger.debug("value is present in primary key from before");
                            }
                        }
                        tryGeneratingUniqueValueCount++;
                    }
                    repeatForColumnsLogger.debug("isColumnPrimaryKey("+columnName+") => "+dataSet.isColumnPrimaryKey(columnName));
                    repeatForColumnsLogger.debug("isValueGeneratedCorrecly => "+isValueGeneratedCorrecly);
                    if (dataSet.isColumnPrimaryKey(columnName) && !isValueGeneratedCorrecly) {
                        repeatForColumnsLogger.debug("columnValue => "+columnValue);
                        //repeatForColumnsLogger.debug("generatedRows => "+generatedRows);
                        repeatForColumnsLogger.error("unique value could not be generated for column ("+dataSet.getName()+"."+columnName+")");
                         
                        throw new Exception("unique value could not be generated for column ("+dataSet.getName()+"."+columnName+")");
                    }
                    row.put(columnName, columnValue);
                    if (dataSet.isColumnPrimaryKey(columnName)) {
                        dataSet.submitPrimaryKeyForColumn(columnName, columnValue);
                    }
                }  else {
                    throw new Exception("dataType cannot be null for column ("+dataSet.getName()+"."+columnName+")");
                }
            }
        }
        return row;
    }

    private static boolean isCombinationAlreadyPresent(HashMap<String, Object> row, HashMap<String, Object> combination) throws Exception {
        for (String key : combination.keySet()) {
            Object rowValue = row.get(key);
            Object combinationValue = combination.get(key);
            if (!rowValue.equals(combinationValue)) {
                return false;
            }
        }
        return true;
    }

    // this is given an array list of columns and their unique values
    // eg, col1 : [1, 2], col2 : [A, B], ...
    // it returns an array list of hashmap with unique combination of values
    // [{col1 : 1, col2 : A},{col1 : 1, col2 : B},{col1 : 2, col2 : A},{col1 : 2, col2 : B}]
    public static HashSet<HashMap> createCombinations(ArrayList<UniqueColumnValuesTuple> selectedCombinationColumns, ArrayList<String> combinationColumnNames, HashSet<HashMap> toReturn, HashMap combination) throws Exception {
        repeatForColumnsLogger.debug("\n\n");
        repeatForColumnsLogger.debug("selectedCombinationColumns ("+selectedCombinationColumns+")");
        repeatForColumnsLogger.debug("combinationColumnNames ("+combinationColumnNames+")");
        repeatForColumnsLogger.debug("toReturn ("+toReturn+")");
        repeatForColumnsLogger.debug("combination ("+combination+")");

        if (toReturn == null) {
            toReturn = new HashSet();
        }
        if (selectedCombinationColumns.size() > 0) {
            UniqueColumnValuesTuple uniqueValues = selectedCombinationColumns.get(0);
            if (selectedCombinationColumns.size() > 1) {
                selectedCombinationColumns = new ArrayList(selectedCombinationColumns.subList(1, selectedCombinationColumns.size()));
            } else {
                selectedCombinationColumns = new ArrayList();
            }
            
            repeatForColumnsLogger.debug("selected uniqueValues ("+uniqueValues+")");
            for (Object uniqueValue : uniqueValues.uniqueValues) {
                repeatForColumnsLogger.debug("uniqueValue ("+uniqueValue+")");
                repeatForColumnsLogger.debug("combination ("+combination+")");
                if (combination == null) {
                    combination = findNonCompleteCombination(toReturn, combinationColumnNames);
                    repeatForColumnsLogger.debug("non complete combination ("+combination+")");
                    if (combination == null) {
                        combination = new HashMap();
                    }
                    repeatForColumnsLogger.debug("selected combination ("+combination+")");
                }
                combination.put(uniqueValues.columnName, uniqueValue);
                repeatForColumnsLogger.debug("filled combination ("+combination+")");
                repeatForColumnsLogger.debug("calling nested loop unique ("+uniqueValue+")...");
                if (selectedCombinationColumns.size() > 0) {
                    createCombinations(selectedCombinationColumns, combinationColumnNames, toReturn, combination);    
                }
                repeatForColumnsLogger.debug("nest call finished loop unique ("+uniqueValue+")....");
                toReturn.add(combination);
                repeatForColumnsLogger.debug("toReturn (end loop) ("+toReturn+") loop unique ("+uniqueValue+")");
                combination = (HashMap)combination.clone();
            }
        }
        repeatForColumnsLogger.debug("toReturn (end function) ("+toReturn+")\n\n");
        return toReturn;
    }

    // find the combination map from hashset that does not have all the keys in combination column maps
    private static HashMap findNonCompleteCombination(HashSet<HashMap> combinations, ArrayList<String> combinationColumnNames) {
        for (HashMap combination : combinations) {
            for(String columnName : combinationColumnNames) {
                if (!combination.keySet().contains(columnName)) {
                    return combination;
                }
            }
        }
        return null;
    }
}