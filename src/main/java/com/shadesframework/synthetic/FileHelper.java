package com.shadesframework.synthetic;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileHelper {

    private static Logger logger = LogManager.getLogger(FileHelper.class);

    public static Collection<String> getResources(
        final Pattern pattern){
        final ArrayList<String> retval = new ArrayList<String>();
        final String classPath = System.getProperty("java.class.path", ".");
        final String[] classPathElements = classPath.split(System.getProperty("path.separator"));
        for(final String element : classPathElements){
            retval.addAll(getResources(element, pattern));
        }
        return retval;
    }

    private static Collection<String> getResources(
        final String element,
        final Pattern pattern){
        final ArrayList<String> retval = new ArrayList<String>();
        final File file = new File(element);
        if(file.isDirectory()){
            retval.addAll(getResourcesFromDirectory(file, pattern));
        } else{
            retval.addAll(getResourcesFromJarFile(file, pattern));
        }
        return retval;
    }

    private static Collection<String> getResourcesFromJarFile(
        final File file,
        final Pattern pattern){
        final ArrayList<String> retval = new ArrayList<String>();
        ZipFile zf;
        try{
            zf = new ZipFile(file);
        } catch(final ZipException e){
            throw new Error(e);
        } catch(final IOException e){
            throw new Error(e);
        }
        final Enumeration e = zf.entries();
        while(e.hasMoreElements()){
            final ZipEntry ze = (ZipEntry) e.nextElement();
            final String fileName = ze.getName();
            final boolean accept = pattern.matcher(fileName).matches();
            if(accept){
                retval.add(fileName);
            }
        }
        try{
            zf.close();
        } catch(final IOException e1){
            throw new Error(e1);
        }
        return retval;
    }

    private static Collection<String> getResourcesFromDirectory(
        final File directory,
        final Pattern pattern){
        final ArrayList<String> retval = new ArrayList<String>();
        final File[] fileList = directory.listFiles();
        for(final File file : fileList){
            if(file.isDirectory()){
                retval.addAll(getResourcesFromDirectory(file, pattern));
            } else{
                try{
                    final String fileName = file.getCanonicalPath();
                    final boolean accept = pattern.matcher(fileName).matches();
                    if(accept){
                        retval.add(fileName);
                    }
                } catch(final IOException e){
                    throw new Error(e);
                }
            }
        }
        return retval;
    }

    public static String readFileContent(String fileWithPath) throws Exception {
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(fileWithPath));
            String content = new String(encoded, StandardCharsets.UTF_8);
            return content;
        } catch(Exception e) {
            InputStream stream = FileHelper.class.getClassLoader().getResourceAsStream(fileWithPath);
            byte[] encoded = getBytes(stream);
            String content = new String(encoded, StandardCharsets.UTF_8);
            return content;
        }
    }

    public static String getFileNameFromFullPath(String path) throws Exception {
        File file = new File(path);
        String fileName = file.getName();
        if (fileName.indexOf(".") > 0) {
            fileName = fileName.substring(0, fileName.lastIndexOf("."));
        }
        return fileName;
    }

    public static byte[] getBytes(InputStream is) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[4];
    
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
    
        buffer.flush();
        byte[] targetArray = buffer.toByteArray();
        return targetArray;
    }
}  