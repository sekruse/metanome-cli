# Metanome CLI

The main purpose of this project is to provide a command-line interface for [Metanome](https://github.com/HPI-Information-Systems/Metanome) to allow for easy automation of data profiling tasks, e.g., to conduct experiments or to profile datasets batchwise.
Besides that, this project integrates Metanome with
* [Metacrate](https://github.com/stratosphere/metadata-ms), a storage and analytics tool for data profiles, and
* [ProfileDB](https://github.com/sekruse/profiledb-java), a tiny tool to collect and store experimental data.

Furthermore, HDFS is supported as input source.

## Installation

The Metanome CLI can be built with Maven:
```
.../metanome-cli$ mvn package -Pdistro
```
This command creates a "fatjar" (`target/metanome-cli-0.1-SNAPSHOT.jar` or similar) that contains Metanome and the Metanome CLI along with all their dependencies (except for Metanome algorithms, though).

Note that this project might depend on unstable snapshot versions of [Metanome](https://github.com/HPI-Information-Systems/Metanome), [Metacrate](https://github.com/stratosphere/metadata-ms), and [ProfileDB](https://github.com/sekruse/profiledb-java).
In case of build errors related to these projects, you might need to clone, build, and install (i.e., `mvn install`) them yourself.
Then, re-run the build with
```
.../metanome-cli$ mvn package -Pdistro --offline
```

## Usage

Once you have obtained above described fatjar, you can simply put it on the Java classpath along with your algorithm jar files and run them as a normal Java application.
As an example, assume you have an algorithm jar file called `my-algorithm.jar` with the main algorithm class `com.example.MyAlgorithm`.
Then you can execute it via
```
$ java -cp metanome-cli.jar:my-algorithm.jar de.metanome.cli.App --algorithm com.example.MyAlgorithm <parameters...>
```
To learn about the various parameters of the Metanome CLI, you can also execute it without any parameters (including `--algorithm`) and get an output like the following:
```
Usage: <main class> [options]
  Options:
  * -a, --algorithm
       name of the Metanome algorithm class
    --algorithm-config
       algorithm configuration parameters (<name>:<value>)
       Default: []
    --db-connection
       a PGPASS file that specifies the database connection; if given, the
       inputs are treated as database tables
    --db-type
       the type of database as it would appear in a JDBC URL
    --escape
       escapes special characters
       Default:
    --header
       first row is a header
       Default: false
    --ignore-leading-spaces
       ignore leading white spaces in each field
       Default: false
  * --file-key, --input-key, --table-key
       configuration key for the input files/tables
  * --files, --inputs, --tables
       input file/tables to be analyzed and/or files list input files/tables
       (prefixed with 'load:')
       Default: []
    --null
       representation of NULLs
       Default: <empty string>
    -o, --output
       how to output results (none/print/file[:run-ID]/crate:file:scope)
       Default: file
    --profiledb
       location of a ProfileDB to store a ProfileDB experiment at
    --profiledb-conf
       additional configuration to store with a ProfileDB experiment
       Default: []
    --profiledb-key
       experiment key to store a ProfileDB experiment
    --profiledb-tags
       tags to store with a ProfileDB experiment
       Default: []
    --quote
       delimits fields in the input file
       Default: "
    --separator
       separates fields in the input file
       Default: ;
    --skip
       numbers of lines to skip
       Default: 0
    --skip-differing-lines
       skip lines with incorrect number of fields
       Default: false
    --strict-quotes
       enforce strict quotes
       Default: false
```

In case of problems, feel free to file an issue.
