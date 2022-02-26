package com.shadesframework;

import com.shadesframework.shadesdata.FileHelper;
import com.shadesframework.shadesdata.FileMetadata;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    /**
     * Rigorous Test :-)
     */
    @Test
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
            }
            System.out.println("\n\n\n\n");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
