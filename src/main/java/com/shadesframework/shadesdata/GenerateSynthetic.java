package com.shadesframework.shadesdata;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GenerateSynthetic {
    private static Logger logger = LogManager.getLogger(GenerateSynthetic.class);
    public static void main(final String[] args) {
        try {
            FileMetadata fmd = new FileMetadata();
            ArrayList<DataSet> dataSets = fmd.getConfiguredDataSets();
            logger.debug("unsorted datasets => "+dataSets);
            Collections.sort(dataSets);
            logger.debug("sorted datasets => "+dataSets);
            ArrayList<String> dataSetsAlreadyGeneratedRowsFor = new ArrayList();
            for (DataSet dataSet : dataSets) {
                if (!dataSetsAlreadyGeneratedRowsFor.contains(dataSet.getName())) {
                    ArrayList<String> generatedDataSets = dataSet.generateRows();
                    dataSetsAlreadyGeneratedRowsFor.add(dataSet.getName());
                    for (String generatedDataSetName : generatedDataSets) {
                        if (!dataSetsAlreadyGeneratedRowsFor.contains(generatedDataSetName)) {
                            dataSetsAlreadyGeneratedRowsFor.add(generatedDataSetName);
                        }
                    }
                }
                logger.debug("dataSetsAlreadyGeneratedRowsFor => "+dataSetsAlreadyGeneratedRowsFor);
                logger.debug("data set => "+dataSet.getName());
                logger.debug("rows => "+dataSet.getGeneratedRows());
            }
            for (DataSet dataSet : dataSets) {
                dataSet.storeRows();
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}