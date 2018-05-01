import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import tane_impl.*;

public class TaneMain
{
	public static void main( String[] args )
    {
		SparkConf sparkConf = new SparkConf().setAppName("DistributedTane");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		/*Tane.sc = new JavaSparkContext(sparkConf);
		Tane.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		Tane.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("TaneMain")
				  .getOrCreate();
		Tane.df = spark.read().json(Tane.datasetFile);
		Tane.df.printSchema();
		Tane.columnNames = Tane.df.columns();
		Tane.numberAttributes = Tane.columnNames.length-1;
		Long start = System.currentTimeMillis();
		Tane.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        Tane.sc.stop();*/
		
		DistributedTane1.sc = new JavaSparkContext(sparkConf); 
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println("batch size: "+args[1]);
		System.out.println("numPartition: "+args[2]);
		//System.out.println("numSparkTasksFactor: "+args[2]);
		DistributedTane1.datasetFile = args[0];
		DistributedTane1.batch_size = Integer.parseInt(args[1]);
		DistributedTane1.numPartitions = Integer.parseInt(args[2]);
		//DistributedTane1.numSparkTasksFactor = Integer.parseInt(args[2]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("TaneMain")
				  .getOrCreate();
		DistributedTane1.df = spark.read().json(DistributedTane1.datasetFile);
		//DistributedTane1.df.printSchema();
		DistributedTane1.columnNames = DistributedTane1.df.columns();
		DistributedTane1.numberAttributes = DistributedTane1.columnNames.length-1;
		DistributedTane1.df.cache();
		DistributedTane1.numberTuples = DistributedTane1.df.count();
		Long start = System.currentTimeMillis();
		DistributedTane1.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        DistributedTane1.sc.stop();
        
		/*DistributedTane2.sc = new JavaSparkContext(sparkConf); 
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println("batch size: "+args[1]);
		System.out.println("numPartition: "+args[2]);
		DistributedTane2.datasetFile = args[0];
		DistributedTane2.batch_size = Integer.parseInt(args[1]);
		DistributedTane2.numPartitions = Integer.parseInt(args[2]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("TaneMain")
				  .getOrCreate();
		DistributedTane2.df = spark.read().json(DistributedTane2.datasetFile);
		//DistributedTane1.df.printSchema();
		DistributedTane2.columnNames = DistributedTane2.df.columns();
		DistributedTane2.numberAttributes = DistributedTane2.columnNames.length-1;
		DistributedTane2.df.cache();
		DistributedTane2.numberTuples = DistributedTane2.df.count();
		Long start = System.currentTimeMillis();
		DistributedTane2.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        DistributedTane2.sc.stop();*/

    }
}
