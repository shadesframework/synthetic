package com.shadesframework.shadesdata;

import java.math.BigDecimal;
import java.util.Collection;
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

}