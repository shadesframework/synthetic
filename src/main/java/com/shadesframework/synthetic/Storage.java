package com.shadesframework.synthetic;


public interface Storage {
    public void createDataSetContainer(DataSet dataSet) throws Exception;
    public void storeRows(DataSet dataSet) throws Exception;
}