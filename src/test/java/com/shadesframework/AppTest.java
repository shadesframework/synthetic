package com.shadesframework;

import com.shadesframework.shadesdata.FileHelper;
import com.shadesframework.shadesdata.FileMetadata;
import java.util.Collection;
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

    @Test
    public void testFileMetaReader() {
        try {
            FileMetadata fmd = new FileMetadata();
            fmd.getConfiguredDataSets(true);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
