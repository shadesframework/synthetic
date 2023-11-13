package com.shadesframework.synthetic;
import java.util.HashSet;

public class UniqueColumnValuesTuple {
    String columnName;
    HashSet<String> uniqueValues;
    
    public UniqueColumnValuesTuple(String column, HashSet uniqueVal) {
        this.columnName = column;
        this.uniqueValues = uniqueVal;
    }

    @Override
    public String toString() {
        return this.columnName+"::"+uniqueValues;
    }
}