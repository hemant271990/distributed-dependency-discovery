//package uw.distributed_fastfd;

import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import fastfd_impl.*;
import fastfd_helper.*;

public class FastFDMain
{
    public static void main( String[] args )
    {
    	// Global declarations
        SparkConf sparkConf = new SparkConf().setAppName("DistributedFastFD");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		// Implementation specific declarations
        
        /*FastFD.sc = new JavaSparkContext(master, "FastFDMain", sparkConf);
		FastFD.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		FastFD.datasetFile = args[1];
		//mode = Integer.parseInt(args[3]);
		SQLContext sqlContext = new SQLContext(FastFD.sc);
		FastFD.df = sqlContext.jsonFile(FastFD.datasetFile);
		FastFD.df.printSchema();
		FastFD.columnNames = FastFD.df.columns();
		FastFD.numberAttributes = FastFD.columnNames.length-1;
		FastFD.execute();
		FastFD.sc.stop();*/ 
		/*
		DistributedFastFD.sc = new JavaSparkContext(master, "FastFDMain", sparkConf);
		DistributedFastFD.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD.datasetFile = args[1];
		//mode = Integer.parseInt(args[3]);
		SQLContext sqlContext = new SQLContext(DistributedFastFD.sc);
		DistributedFastFD.df = sqlContext.jsonFile(DistributedFastFD.datasetFile);
		DistributedFastFD.df.printSchema();
		DistributedFastFD.columnNames = DistributedFastFD.df.columns();
		DistributedFastFD.numberAttributes = DistributedFastFD.columnNames.length;
		DistributedFastFD.execute();
		DistributedFastFD.sc.stop();*/
		
		/*DistributedFastFD2.sc = new JavaSparkContext(master, "FastFDMain", sparkConf);
		DistributedFastFD2.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD2.datasetFile = args[1];
		//mode = Integer.parseInt(args[3]);
		SQLContext sqlContext = new SQLContext(DistributedFastFD2.sc);
		DistributedFastFD2.df = sqlContext.jsonFile(DistributedFastFD2.datasetFile);
		DistributedFastFD2.df.printSchema();
		DistributedFastFD2.columnNames = DistributedFastFD2.df.columns();
		DistributedFastFD2.numberAttributes = DistributedFastFD2.columnNames.length-1;
		DistributedFastFD2.execute();
		DistributedFastFD2.sc.stop();*/
		
		/*DistributedFastFD3.sc = new JavaSparkContext(master, "FastFDMain", sparkConf);
		DistributedFastFD3.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD3.datasetFile = args[1];
		//mode = Integer.parseInt(args[3]);
		SQLContext sqlContext = new SQLContext(DistributedFastFD3.sc);
		DistributedFastFD3.df = sqlContext.jsonFile(DistributedFastFD3.datasetFile);
		DistributedFastFD3.df.printSchema();
		DistributedFastFD3.columnNames = DistributedFastFD3.df.columns();
		DistributedFastFD3.numberAttributes = DistributedFastFD3.columnNames.length;
		DistributedFastFD3.execute();
		DistributedFastFD3.sc.stop();*/
		
		/*DistributedFastFD4.sc = new JavaSparkContext(sparkConf);
		DistributedFastFD4.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD4.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		DistributedFastFD4.df = spark.read().json(DistributedFastFD4.datasetFile);
		DistributedFastFD4.df.printSchema();
		DistributedFastFD4.columnNames = DistributedFastFD4.df.columns();
		DistributedFastFD4.numberAttributes = DistributedFastFD4.columnNames.length-1;
		DistributedFastFD4.execute();
		DistributedFastFD4.sc.stop();*/
		
		/*DistributedFastFD5.sc = new JavaSparkContext(sparkConf);
		DistributedFastFD5.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD5.datasetFile = args[0];
		DistributedFastFD5.spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		DistributedFastFD5.df = DistributedFastFD5.spark.read().json(DistributedFastFD5.datasetFile);
		DistributedFastFD5.df.printSchema();
		DistributedFastFD5.columnNames = DistributedFastFD5.df.columns();
		DistributedFastFD5.numberAttributes = DistributedFastFD5.columnNames.length - 1;
		DistributedFastFD5.execute();
		DistributedFastFD5.sc.stop();*/
		
		/*DistributedFastFD6.sc = new JavaSparkContext(sparkConf);
		DistributedFastFD6.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD6.datasetFile = args[0];
		DistributedFastFD6.numPartitions = Integer.parseInt(args[1]);
		System.out.println(" ** Start time: "+(new Date()).toString());

		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		
		DistributedFastFD6.df = spark.read().json(DistributedFastFD6.datasetFile);
		DistributedFastFD6.df.printSchema();
		DistributedFastFD6.columnNames = DistributedFastFD6.df.columns();
		DistributedFastFD6.numberAttributes = DistributedFastFD6.columnNames.length - 1;
		DistributedFastFD6.execute();
		DistributedFastFD6.sc.stop();*/
		
		/*DistributedFastFD7.sc = new JavaSparkContext(sparkConf);
		DistributedFastFD7.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD7.datasetFile = args[0];
		DistributedFastFD7.numReducers = Integer.parseInt(args[1]);
		DistributedFastFD7.numPartitions = DistributedFastFD7.numReducers;
		System.out.println(" ** Start time: "+(new Date()).toString());
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		DistributedFastFD7.df = spark.read().json(DistributedFastFD7.datasetFile);
		DistributedFastFD7.df.printSchema();
		DistributedFastFD7.columnNames = DistributedFastFD7.df.columns();
		DistributedFastFD7.numberAttributes = DistributedFastFD7.columnNames.length - 1;
		DistributedFastFD7.execute();
		DistributedFastFD7.sc.stop();*/
		
		/*DistributedFastFD8.sc = new JavaSparkContext(sparkConf);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD8.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		DistributedFastFD8.df = spark.read().json(DistributedFastFD8.datasetFile);
		DistributedFastFD8.df.printSchema();
		DistributedFastFD8.columnNames = DistributedFastFD8.df.columns();
		DistributedFastFD8.numberAttributes = DistributedFastFD8.columnNames.length-1;
		DistributedFastFD8.execute();
		DistributedFastFD8.sc.stop();*/
		
		/*DistributedFastFD9.sc = new JavaSparkContext(sparkConf);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD9.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		DistributedFastFD9.df = spark.read().json(DistributedFastFD9.datasetFile);
		DistributedFastFD9.df.printSchema();
		DistributedFastFD9.columnNames = DistributedFastFD9.df.columns();
		DistributedFastFD9.numberAttributes = DistributedFastFD9.columnNames.length-1;
		DistributedFastFD9.execute();
		DistributedFastFD9.sc.stop();*/
		
		/*DistributedFastFD10.sc = new JavaSparkContext(sparkConf);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD10.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		DistributedFastFD10.df = spark.read().json(DistributedFastFD10.datasetFile);
		DistributedFastFD10.df.printSchema();
		DistributedFastFD10.columnNames = DistributedFastFD10.df.columns();
		DistributedFastFD10.numberAttributes = DistributedFastFD10.columnNames.length-1;
		DistributedFastFD10.execute();
		DistributedFastFD10.sc.stop();*/
		
		DistributedFastFD11.sc = new JavaSparkContext(sparkConf);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFastFD11.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastFDMain")
				  .getOrCreate();
		DistributedFastFD11.df = spark.read().json(DistributedFastFD11.datasetFile);
		DistributedFastFD11.df.printSchema();
		DistributedFastFD11.columnNames = DistributedFastFD11.df.columns();
		DistributedFastFD11.numberAttributes = DistributedFastFD11.columnNames.length-1;
		DistributedFastFD11.execute();
		DistributedFastFD11.sc.stop();
    }
}
