# Data in the Fast Lane

## What can I do with Data in the Fast Lane (DFL)?

![What is Data in the Fast Lane](https://github.com/AdvanceAutoParts/datafastlane/blob/master/doc/What%20is%20Data%20in%20the%20Fast%20Lane.png?raw=true)

## What is Data in the Fast Lane (DFL)?

TBD

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
