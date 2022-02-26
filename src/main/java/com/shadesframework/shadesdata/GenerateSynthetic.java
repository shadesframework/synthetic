package com.shadesframework.shadesdata;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GenerateSynthetic {
    private static Logger logger = LogManager.getLogger(GenerateSynthetic.class);
    public static void main(final String[] args) {
        try {
            FileMetadata fmd = new FileMetadata();
            ArrayList<DataSet> dataSets = fmd.getConfiguredDataSets();
            ArrayList<String> dataSetsAlreadyGeneratedRowsFor = new ArrayList();
            for (DataSet dataSet : dataSets) {
                if (!dataSetsAlreadyGeneratedRowsFor.contains(dataSet.getName())) {
                    dataSetsAlreadyGeneratedRowsFor.add(dataSet.getName());
                    dataSet.generateRows();
                }
                logger.debug("data set => "+dataSet.getName());
                logger.debug("rows => "+dataSet.getGeneratedRows());
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}