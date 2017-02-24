# HareDBWebRESTful #

HareDBWebRESTful is a RESTful web services of HBase, and using the same engine with HareDBHBaseClient.

### General Info ###

For the latest information about HareDBHBaseClient, please visit out website at:

[www.haredb.com](http://www.haredb.com)

### Requirements ###

- Java 1.6, 1.7
- Hadoop 2.x
- HBase 0.98.x
- Apache Tomcat 7.x or other Web container

## Getting Started ##

* Download and unzip source code, then building war file from source code with Gradle .

* `cd HareDBWebRESTful`
* `gradle clean build -x test`
* `cd HareDBWebRESTful/HareDB_HBaseClient_WebRestful/build/libs`

* Deploy the war file in your container.(Ex:Tomcat)

    * Getting HareSpark.jar from HareDB_HBaseClient_FacadeAPI/libs/haredb/HareSpark.jar
    * Creating a folder in ${Tomcat_Home}/HareRestfulConfig and create a properties file named 'sysconfig.properties' in folder.
    * Adding ${Tomcat_Home}/HareRestfulConfig to tomcat classpath
    * Here is the sysconfig.properties :

```
sparkcommoncsvJarPath=${local_path}/commons-csv-1.2.jar
hareSparkAssemblyJarPath=${local_path}/HareSpark.jar
sparkcsvJarPath=${local_path}/spark-csv_2.10-1.2.0.jar
metaFolderName=.meta
productName=harespark
hdfsTableFolderRoot=/home/user1
sparkAssemblyJarPath=hdfs\://server\:9000/spark-assembly-1.4.0-hadoop2.3.0.jar
```

## Release Note ##

* 1.98.06.03 -
    * Fixed bugs.
    * Supporting Spark sql.

* 1.98.06 – Bugs fixed、support meta table and Sentry
    * When a meta table created, Hareql support meta table name.

* 1.98.05 - Bugs fixed and support kerberos.

* 1.98.01 - Feature Support:
    * HBase API Scan、Put.
    * Backload Data.
    * [HareQL](http://www.haredb.com/HareDB/src_ap/Product_HareDBClient_QL.aspx?l=4) (Sql like language).






h2. Note

* Q: When I call the API, return "connection is null,please reconnection".
** A: Please reconnect with the {hostname}/connect API.

* Q: Upload data to hbase fail.
** A: Please make sure the data file and schema file have already uploaded to your HDFS.
