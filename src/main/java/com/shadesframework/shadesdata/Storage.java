package com.shadesframework.shadesdata;


public interface Storage {
    public void createDataSetContainer(DataSet dataSet) throws Exception;
    public void storeRows(DataSet dataSet) throws Exception;
}