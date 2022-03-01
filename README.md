# shadesdata
shadesdata is a synthetic data generation framework supporting relational data model.

[["IT IS EXPERIMENTAL"]]

You need to provide .synth files, in the classpath, to configure synthetic data generation.
It supports relational data generation

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
            "thisColumn" : "<columnName>" // this column needs to be primary key in this dataset
        }],
        "oneToMany" : [{
            "relatedDataset" : "<relatedDatasetName>",
            "relatedColumn" : "<columnName>" // this column needs to be foreign key in "relatedDataset",
            "multiplicityGuidance" : "3" // this needs to be given for related Many side. Eg, 3 rows will be generated for related 'many' side for each row generated for 'this' side
            "thisColumn" : "<columnName>" // this column needs to be primary key in this dataset
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
        "type" : // "json", "csv" (default), OR "db", [only csv and db are supported at the moment]
        "pointer" : // "filepath" (default "./") OR "jdbc url" (default "jdbc:h2:file:~/test"),
        "rows" : "<rows>" // default 10
    }
}
```

Example :::

Suppose we want to generate products, customers and orders matching following requirements

1. each product could have many orders generated for it
2. each order will belong to a unique customer (customer will have many orders)
3. product names will be saved in column "product" and will have regex pattern 'product[0-9][0-9][0-9]'
4. product price will depend on the product names
    4i. if product = product111, price will range between 10 and 19
    4ii if product = product211, price will range between 20 and 39
    4iii for all other product names, price will range between 50 and 100
5. product price will be saved in column 'price'
6. generated customer names will be selected from a rotating list
7. generate 100 rows of data for each of these entities.
8. set primary and foreign key relations, in the generated data, according to #1 and #2 above

The configuration for this example is realized in the resources folder as files order.synth, customer.synth and product.synth

you can build the maven project and run it as 

```
java -jar ./target/shadesdata-0.0.1-SNAPSHOT.jar
2022-03-01 07:53:37 [com.shadesframework.shadesdata.Synthetic.generate:45] INFO data set (product) generated and stored
2022-03-01 07:53:37 [com.shadesframework.shadesdata.Synthetic.generate:45] INFO data set (customer) generated and stored
2022-03-01 07:53:37 [com.shadesframework.shadesdata.Synthetic.generate:45] INFO data set (order) generated and stored
```

The datasets will be generated in the location pointed to by storage.pointer property of each entity.

There is one example to showcase storing generated data in database (see testdbdataset.synth for jdbc configuration)

You can also use this library for data generation for your unit testing. Just provide the necessary .synth files in the classpath and call Synthetic.generate() from your test case initializations.

Happy data generation!!!

We are still evolving this. Keep checking back.
Write to 'shadesframework@yandex.com' for bugs, suggestions and support.