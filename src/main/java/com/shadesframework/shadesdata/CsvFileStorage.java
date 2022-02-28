package com.shadesframework.shadesdata;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

public class CsvFileStorage implements Storage {
    String storagePath = "";

    public CsvFileStorage(String storageLocation) {
        this.storagePath = storageLocation;
    }
    
    @Override
    public void createDataSetContainer(DataSet dataSet) throws Exception {
        String fileNameWithPath = storagePath+File.separator+dataSet.getName()+".csv";
        File file = new File(fileNameWithPath);
        file.delete();
        file.createNewFile();
    }

    @Override
    public void storeRows(DataSet dataSet) throws Exception {
        String fileNameWithPath = storagePath+File.separator+dataSet.getName()+".csv";
        ArrayList<String> header = dataSet.getColumns();
        String rowStr = String.join(",", header)+"\r\n";
        Files.write(Paths.get(fileNameWithPath), rowStr.getBytes(), StandardOpenOption.APPEND);

        for (HashMap row : dataSet.getGeneratedRows()) {
            HashMap rowClone = (HashMap)row.clone();
            rowClone.keySet().removeIf(value -> value.toString().matches("^@.*"));
            rowStr = (String)rowClone.values().stream().map(Object::toString).collect(Collectors.joining(","))+"\r\n";
            Files.write(Paths.get(fileNameWithPath), rowStr.getBytes(), StandardOpenOption.APPEND);
        }
    }
}