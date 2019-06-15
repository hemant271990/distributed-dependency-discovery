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
		
		// LDP1 in paper
		DistributedFastFD10.sc = new JavaSparkContext(sparkConf);
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
		DistributedFastFD10.sc.stop();
		
		
    }
}
