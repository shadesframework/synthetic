package com.shadesframework.shadesdata;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CommonHelper {
    private static Logger logger = LogManager.getLogger(CommonHelper.class);
    public static boolean isCollectionOfType(Collection collection, String type) {
        for (Object o : collection) {
            if (!o.getClass().getName().equals(type)) {
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

}