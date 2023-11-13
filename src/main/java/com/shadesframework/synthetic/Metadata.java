package com.shadesframework.synthetic;
import java.util.ArrayList;


public interface Metadata {
    public ArrayList<DataSet> getConfiguredDataSets(boolean generateExamples) throws Exception;
    public DataSet getDataSet(String dataSetName) throws Exception;
}