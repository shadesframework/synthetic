package com.shadesframework.shadesdata;
import com.mifmif.common.regex.Generex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;


public class DataGenHelper {
    
    public static String generateString(String columnName, HashMap format, HashMap row, HashMap previousRow) throws Exception {
        String generatedString = null;
        Object regExp = MetaDataHelper.getColumnFormatParameterValue("regexBased",format, row);
        if (regExp != null) {
            generatedString = generateRandomString((String)regExp);
        }

        Object randomPick = MetaDataHelper.getColumnFormatParameterValue("randomPick",format, row);
        if (randomPick != null) {
            generatedString = pickRandomStringFromList((ArrayList<String>)randomPick);
        }

        Object rotate = MetaDataHelper.getColumnFormatParameterValue("rotate",format, row);
        if (rotate != null) {
            generatedString = pickNextStringFromList((ArrayList<String>)rotate, columnName, previousRow);
        }

        throw new Exception("don't know how to generate string for column ("+columnName+")");
    }

    private static String generateRandomString(String regExp) throws Exception {
        Generex generex = new Generex(regExp);
		String randomStr = generex.random();
        return randomStr;
    }

    private static String pickRandomStringFromList(ArrayList<String> stringList) throws Exception {
        int randomIndex = ThreadLocalRandom.current().nextInt(0, stringList.size());
        return stringList.get(randomIndex);
    }

    private static String pickNextStringFromList(ArrayList<String> stringList, String columnName, HashMap previousRow) throws Exception {
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
            Number number = (Number)previousRow.get(columnName);
            if (number == null) {
                throw new Exception("number doesnt exist for previous row");
            }
            double spacing = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("spacingFromPreviousRow",format, row));
            boolean randomSpacing = Boolean.parseBoolean((String)MetaDataHelper.getColumnFormatParameterValue("randomSpacing",format, row));
            String increment = (String)MetaDataHelper.getColumnFormatParameterValue("applySpacingWithIncrement",format, row);
            double rangeStart = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row));
            double rangeEnd = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeEnd",format, row));
        
            double genenratedNumber = generateNumberWithSpacing(number.doubleValue(), spacing, randomSpacing, increment, rangeStart, rangeEnd);
            return genenratedNumber;
        } else {
            if(isNumberRangeBased(format, row)) {
                double rangeStart = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeStart",format, row));
                double rangeEnd = Double.parseDouble((String)MetaDataHelper.getColumnFormatParameterValue("rangeEnd",format, row));
                double genenratedNumber = generateRandomNumber(rangeStart, rangeEnd);
                return genenratedNumber;
            }
            if (isNumberSynthetic(format, row)) {
                int digitsBeforeDecimal = Integer.parseInt((String)MetaDataHelper.getColumnFormatParameterValue("digitsBeforeDecimal",format, row));
                int digitsAfterDecimal = Integer.parseInt((String)MetaDataHelper.getColumnFormatParameterValue("digitsAfterDecimal",format, row));
                double genenratedNumber = generateRandomNumber(digitsBeforeDecimal, digitsBeforeDecimal);
                return genenratedNumber;
            }
        }
        throw new Exception("don't know how to generate number for column ("+columnName+")");
    }

    private static double generateNumberWithSpacing(double originalNumber, double spacing, 
        boolean randomSpacing, String increment, double rangeStart, double rangeEnd) throws Exception {
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
        
        double toReturn = option1;
        boolean shouldIncrement = false;
        boolean ignore = true;

        if (increment != null) {
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
            }
            if (!ignore && !shouldIncrement) {
                if (option2 >= rangeStart && option2 <= rangeEnd) {
                    toReturn = option2;
                }
            }
            throw new Exception("could not generate a number within specified range");
        }
        return toReturn;
    }

    private static double generateRandomNumber(int digitsBeforeDecimal, int digitsAfterDecimal) throws Exception {
        long beforeDecimal = ThreadLocalRandom.current().nextLong(getMinLongOfGivenDigits(digitsBeforeDecimal), getMaxLongOfGivenDigits(digitsBeforeDecimal));
        long afterDecimal = ThreadLocalRandom.current().nextLong(getMinLongOfGivenDigits(digitsAfterDecimal), getMaxLongOfGivenDigits(digitsAfterDecimal));
        String number = ""+beforeDecimal+"."+afterDecimal;
        return Double.parseDouble(number);
    }

    private static double generateRandomNumber(double rangeStart, double rangeEnd) throws Exception {
        return ThreadLocalRandom.current().nextDouble(rangeStart, rangeEnd);
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
}