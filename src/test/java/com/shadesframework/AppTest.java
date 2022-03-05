package com.shadesframework;

import com.shadesframework.shadesdata.DataGenHelper;
import com.shadesframework.shadesdata.FileHelper;
import com.shadesframework.shadesdata.FileMetadata;
import com.shadesframework.shadesdata.UniqueColumnValuesTuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.Test;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for simple App.
 */
public class AppTest 
{
    //@Test
    public void shouldAnswerWithTrue()
    {
        try {
            Pattern pattern = Pattern.compile(".*synth");
        
            final Collection<String> list = FileHelper.getResources(pattern);
            System.out.println("\n\n\n\n");
            System.out.println("synth files");
            System.out.println("\n");
            for(final String name : list){
                System.out.println("Name ::"+name+"::");
                System.out.println("::Content::\n\n");
                String content = FileHelper.readFileContent(name);

                FileMetadata fmd = new FileMetadata();
                Map map = fmd.getJsonMap(content);

                System.out.println(map);
                System.out.println("fileName => "+FileHelper.getFileNameFromFullPath(name));
            }
            System.out.println("\n\n\n\n");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void testFileMetaReader() {
        try {
            FileMetadata fmd = new FileMetadata();
            fmd.getConfiguredDataSets(true);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testCombinator() {
        try {
            HashSet<String> uniqueList1 = new HashSet(Arrays.asList("1", "2"));
            HashSet<String> uniqueList2= new HashSet(Arrays.asList("A", "B"));
            HashSet<String> uniqueList3= new HashSet(Arrays.asList("X", "Y", "Z"));

            UniqueColumnValuesTuple tuple1 = new UniqueColumnValuesTuple("col1", uniqueList1);
            UniqueColumnValuesTuple tuple2 = new UniqueColumnValuesTuple("col2", uniqueList2);
            UniqueColumnValuesTuple tuple3 = new UniqueColumnValuesTuple("col3", uniqueList3);
            
            ArrayList<UniqueColumnValuesTuple> combinationColumns = new ArrayList();
            combinationColumns.add(tuple1);
            combinationColumns.add(tuple2);
            combinationColumns.add(tuple3);

            ArrayList<String> combinationColumnNames = new ArrayList(Arrays.asList("col1", "col2", "col3"));

            HashSet combinations = DataGenHelper.createCombinations(combinationColumns, combinationColumnNames, null, null);

            System.out.println("combinations => "+combinations);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
