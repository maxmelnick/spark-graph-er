# Entity Resolution (ER) Using Spark + Graph

This repository contains complementary code for a talk I gave at Spark Summit 2019 on *Massive-Scale Entity Resolution Using Spark + Graph* [link to slides coming soon]. There are simple code examples you can use with Spark to improve your ER, as well as some code examples that will help you better understand how to work with graph in Spark.

There are two sub-projects due to some incompatabilities across associated dependencies. Please refer to the associated README.md in the sub-projects for how to run the examples.

1. `spark-only` - examples ER techniques using Spark GraphFrames. Uses Spark v2.4.
2. `spark-dse` - examples for how to load a DataStax Enterprise (DSE) Graph and perform analytics on the data stored in the graph using out-of-the-box Spark GraphFrame algorithms. Uses Spark v2.2.
