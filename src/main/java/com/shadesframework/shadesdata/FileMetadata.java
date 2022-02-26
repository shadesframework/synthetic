package com.shadesframework.shadesdata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

public class FileMetadata implements Metadata {
    

    @Override
    public ArrayList<DataSet> getConfiguredDataSets() throws Exception {
        Pattern pattern = Pattern.compile(".*synth");
        Collection<String> synthFiles = FileHelper.getResources(pattern);
        for (String synthFile : synthFiles) {
            String fileContent = FileHelper.readFileContent(synthFile);
            
        }
        return null;
    }

    @Override
    public DataSet getDataSet(String dataSetName) {
        return null;
    }

    public Map getJsonMap(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(json, Map.class);
        return map;
    }
    
}