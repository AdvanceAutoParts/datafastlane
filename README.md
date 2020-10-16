# Data in the Fast Lane

## What can I do with Data in the Fast Lane (DFL)?

Data in the Fast Lane (DFL) is a data-oriented tool, design to perform extract, transform, and load (ETL) operations. DFL differentiates itself from other ETL tools as it focuses on scaling thanks to its backing by Apache Spark. DFL does not require any specific infrastructure, it can run on a laptop or leveraging containers in a cloud environment. It can run standalone or embedded in an application. Finally, it is designed to be extensible, with both the data transformation recipes and extension supporting source control.


![What can I do with Data in the Fast Lane](https://github.com/AdvanceAutoParts/datafastlane/blob/master/doc/what_can_i_do_with_data_in_the_fast_lane_dfl.png?raw=true)

DFL is:

 * @fa-gamepad-alt Easy
 * @fa-user-cowboy Standalone
 * !(https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/address-card.svg)

## Installation and first execution

Once you have cloned the source code, run:

    mvn install -Dmaven.test.skip=true
    
Tests are working but may require external components that are not described in the requirements yet. To run Data in the Fast Lane (DFL), simply run:

    dfl.sh <recipe-filename> [--dryrun]

Where your recipe file is a YAML file, like in:

    dfl.sh ./src/test/resources/recipe-count-books.yaml
    
The execution returns: 

```
...
2020-09-08 16:29:26.737 - INFO --- [           main] eaderContext.load(ReaderContext.java:45): >>>> Loading books from file data/books/books.csv
2020-09-08 16:29:30.824 - INFO --- [           main] eaderContext.load(ReaderContext.java:49): >>>> Load completed in 4 seconds
2020-09-08 16:29:31.384 - INFO --- [           main] peration.run(CountRowsOperation.java:35): books has 24 row(s)
2020-09-08 16:29:31.384 - INFO --- [           main] astLaneApp.main(DataFastLaneApp.java:98): DataFastLaneApp completed in 8 seconds with a status of true.
```

## Advanced usage

DFL can also be used as a Java framework. Look at the following examples:

 * `LoadShowLabApp`: executes a basic recipe.
 * `LoadShowTransformLabApp`: executes a recipe with a transformation.
 * `LoadShowJoinLabApp`: executes a recipe with a join between two CSV files.

## More documentation

### User manual

The `doc` directory contains a Microsoft Word user manual. It will be replaced by markdown pages as we move along.

### Blog posts

If you have access to Advance Auto Parts' Confluence, please check out:

 * [End-to-End DFL Tutorial](https://advanceautoparts.atlassian.net/wiki/spaces/OBMS/blog/2020/02/04/985956616/End-to-End+DFL+Tutorial)
 * [Data in the Fast Lane or moving data around at high-speed](https://advanceautoparts.atlassian.net/wiki/spaces/eng/pages/932381558/Data+in+the+Fast+Lane+or+moving+data+around+at+high-speed)
 * [Extending Data in the Fast Lane processing power](https://advanceautoparts.atlassian.net/wiki/spaces/eng/pages/971080327/Extending+Data+in+the+Fast+Lane+processing+power)
 * [Elasticsearch Query Optimization](https://advanceautoparts.atlassian.net/wiki/spaces/eng/pages/1010827890/Elasticsearch+Query+Optimization)
 
Those pages will be also converted to markdown / made available in a public way.
