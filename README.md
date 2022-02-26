# shadesdata
shadesdata is a synthetic data generation framework

You need to provide .synth files, in the classpath, to configure synthetic data generation.
It supports relational data

.synth files will have following json format

```
File <dataset1>.synth

{
    "metaset" : {
        "<columnName>" : { // specify columnName
            "datatype" : // "number" or "string",
            "format" : { 
                // ***** for "number" datatype *****
                "digitsBeforeDecimal" : "3",
                "digitsAfterDecimal" : "4", 
                
                // you can also specify parameter values that depend on other column values example below
                /****alternative example***/
                "digitsAfterDecimal" : [{
                    "parameterValue" : "4" // digitsAfterDecimal should be 4 if dependentColumnName value is peter
                    "expression" : "dependentColumn=='peter'"
                },
                {
                    "parameterValue" : "5" // digitsAfterDecimal should be 5 if the expression below evaluates to true.
                    "expression" : "column1 > 100 and column2 < 10"
                }],
                /*** example end ******/

                // only one combination is allowed. Either digits after and before decimal OR ranges below. Dependency format can be used across all parameters
                "rangeEnd" : "40",
                "rangeStart" : "30", 
                
                "spacingFromPreviousRow" : "4", // the generated number will be spaced by 4 than previous generated row (same column).
                "randomSpacing" : "true", // the spacing above will be chosen at random between 0 and 4.
                "applySpacingWithIncrement" : "true", // if true, the spacing will always be applied with increment on previous row value, otherwise decrement. If this parameter is not specified then it could be either increment or decrement.

                // **** for "string" data type, any one of below ***/
                "randomPick" : ["option1", "option2", "option3", ...], // select random
                "rotate" : ["option1", "option2", "option3", ...] // each row will pick next option
                "regexBased" : "[0-3]([a-c]|[e-g]{1,2})"

            },
            "isPrimaryKey" : // "true" or "false"(default),
            "isForeignKey" : // "true" or "false"(default)
        }
    },
    "metarelations": {
        "oneToOne" : [{
            "relatedDataset" : "<relatedDatasetName>",
            "relatedColumn" : "<columnName>" // this column needs to be primary key in "relatedDataset"
        }],
        "oneToMany" : [{
            "relatedDataset" : "<relatedDatasetName>",
            "relatedColumn" : "<columnName>" // this column needs to be foreign key in "relatedDataset",
            "multiplicityGuidance" : "3" // this needs to be given for related Many side. Eg, 3 rows will be generated for related 'many' side for each row generated for 'this' side
        }],
        "ManyToOne" : [{
            "relatedDataset" : "<relatedDatasetName>",
            "relatedColumn" : "<columnName>" // this column needs to be primary key in "relatedDataset",
            "thisColumn" : <columnName> // this column needs to be foreign key in this dataset
        }],
        "ManyToMany" : [{
            "relatedDataset" : "<relatedDatasetName>",
            "relatedColumn" : "<columnName>" // this column needs to be foreign key in "relatedDataset",
            "thisColumn" : <columnName> // this column needs to be foreign key in this dataset
            "multiplicityGuidance" : "3" // this needs to be given for related Many side. Eg, 3 rows will be generated for related 'many' side for each row generated for 'this' side
        }]
    },
    "storage" : {
        "type" : // "json", "csv" (default), OR "db",
        "pointer" : // "filepath" (default "./") OR "jdbc url",
        "rows" : "<rows>" // default 10
    }
}

```