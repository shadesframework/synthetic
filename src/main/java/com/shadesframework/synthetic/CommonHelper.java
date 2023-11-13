package com.shadesframework.synthetic;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CommonHelper {
    private static Logger logger = LogManager.getLogger(CommonHelper.class);
    public static boolean isCollectionOfType(Collection collection, String type) {
        for (Object o : collection) {
            logger.debug("o.getClass().getName() => "+o.getClass().getName());
            if (type.trim().equals("java.lang.Number")) {
                if (!Number.class.isAssignableFrom(o.getClass())) {
                    return false;
                }
            } else if (!o.getClass().getName().equals(type)) {
                return false;
            }
        }
        return true;
    }

    
    public static double round(double number, int decimalPlace) {
        BigDecimal bd = new BigDecimal(Double.toString(number));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP); 
        logger.debug("bd => "+bd);      
        return bd.doubleValue();
    }

    public static Object selectRandomRow(ArrayList rows) throws Exception {
        int randomIndex = ThreadLocalRandom.current().nextInt(0, rows.size());
        return rows.get(randomIndex);
    }

    public static ArrayList selectRandomItemsNoRepeat(ArrayList list, long numberOfItems) throws Exception {
        HashSet toReturn = new HashSet();
        if (list == null) {
            throw new Exception("list cannot be null");
        }
        if (numberOfItems >= list.size()) {
            return list;
        }
        while(true) {
            if (toReturn.size() < numberOfItems) {
                Object row = selectRandomRow(list);
                toReturn.add(row);
            }
            else {
                break;
            }
        }
        return new ArrayList(toReturn);
    }

    public static Date addDurationToDate(Date date, int duration, String durationType) throws Exception {
        if (date == null) {
            throw new Exception("date cannot be null");
        }
        if (durationType == null) {
            throw new Exception("duration type cannot be null");
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        if (durationType.trim().equals("days")) {
            cal.add(Calendar.DATE, duration);
        } else if (durationType.trim().equals("months")) {
            cal.add(Calendar.MONTH, duration);
        } else if (durationType.trim().equals("weeks")) {
            cal.add(Calendar.WEEK_OF_YEAR, duration);
        } else if (durationType.trim().equals("years")) {
            cal.add(Calendar.YEAR, duration);
        } else {
            throw new Exception("unrecognized duration type ("+durationType+")");
        }
        return cal.getTime();
    }

    public static int daysBetween(Date date1, Date date2) throws Exception {
        if (date1 == null) {
            throw new Exception("date1 cannot be null");
        }
        if (date2 == null) {
            throw new Exception("date2 cannot be null");
        }

        // getting milliseconds for both dates
		long date1InMs = date1.getTime();
		long date2InMs = date2.getTime();
		
		// getting the diff between two dates.
		long timeDiff = 0;
		if(date1InMs > date2InMs) {
			timeDiff = date1InMs - date2InMs;
		} else {
			timeDiff = date2InMs - date1InMs;
		}
		
		// converting diff into days
		int daysDiff = (int) (timeDiff / (1000 * 60 * 60* 24));
        
        return daysDiff;
    }

}