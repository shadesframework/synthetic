package com.shadesframework.shadesdata;
import java.util.ArrayList;


public interface Metadata {
    public ArrayList<DataSet> getConfiguredDataSets() throws Exception;
    public DataSet getDataSet(String dataSetName) throws Exception;
}