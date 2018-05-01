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
		
		DistributedTane1.sc = new JavaSparkContext(sparkConf); 
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println(args[1]);
		System.out.println(args[2]);
		DistributedTane1.datasetFile = args[0];
		//DistributedTane1.numberExecutors = Integer.parseInt(args[1]);
		DistributedTane1.mode = Integer.parseInt(args[2]);
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HybridFDMain")
				  .getOrCreate();
		
		DistributedTane1.df = spark.read().json(DistributedTane1.datasetFile);
		//DistributedTane1.df.printSchema();
		DistributedTane1.columnNames = DistributedTane1.df.columns();
		DistributedTane1.numberAttributes = DistributedTane1.columnNames.length-1;
		DistributedTane1.df.cache();
		DistributedTane1.numberTuples = (new Long(DistributedTane1.df.count())).intValue();
		Long start = System.currentTimeMillis();
		DistributedTane1.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        DistributedTane1.sc.stop();

    }
	
}
