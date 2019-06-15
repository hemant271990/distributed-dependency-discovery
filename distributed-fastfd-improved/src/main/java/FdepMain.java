import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import fdep_impl.*;

public class FdepMain
{
    public static void main( String[] args )
    {
    	// Global declarations
        SparkConf sparkConf = new SparkConf().setAppName("DistributedFdep");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		/*Fdep.sc = new JavaSparkContext(sparkConf);
		Fdep.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		Fdep.datasetFile = args[0];

		SparkSession spark = SparkSession
				  .builder()
				  .appName("FDEPMain")
				  .getOrCreate();
		
		Fdep.df = spark.read().json(Fdep.datasetFile);
		Fdep.df.printSchema();
		Fdep.columnNames = Fdep.df.columns();
		Fdep.numberAttributes = Fdep.columnNames.length-1;
		Fdep.execute();
		Fdep.sc.stop();*/
		
		
		// smPDP
		/*DistributedFdep2.sc = new JavaSparkContext(sparkConf);
		DistributedFdep2.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFdep2.datasetFile = args[0];
		System.out.println(args[0]);

		SparkSession spark = SparkSession
				  .builder()
				  .appName("FDEPMain")
				  .getOrCreate();
		
		DistributedFdep2.df = spark.read().json(DistributedFdep2.datasetFile);
		DistributedFdep2.df.printSchema();
		DistributedFdep2.columnNames = DistributedFdep2.df.columns();
		DistributedFdep2.numberAttributes = DistributedFdep2.columnNames.length-1;
		DistributedFdep2.execute();
		DistributedFdep2.sc.stop();*/
		
		// lmPDP
		DistributedFdep3.sc = new JavaSparkContext(sparkConf);
		DistributedFdep3.sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		DistributedFdep3.datasetFile = args[0];
		DistributedFdep3.numPartitions = Integer.parseInt(args[1]);
		DistributedFdep3.sideLen = Integer.parseInt(args[2]);
		System.out.println(args[0]);
		System.out.println("numPartitions: "+args[1]);
		System.out.println("sideLen: "+args[2]);
		System.out.println(DistributedFdep3.sc.sc().applicationId());

		SparkSession spark = SparkSession
				  .builder()
				  .appName("FDEPMain")
				  .getOrCreate();
		
		DistributedFdep3.df = spark.read().json(DistributedFdep3.datasetFile);
		DistributedFdep3.df.printSchema();
		DistributedFdep3.columnNames = DistributedFdep3.df.columns();
		DistributedFdep3.numberAttributes = DistributedFdep3.columnNames.length-1;
		DistributedFdep3.execute();
		DistributedFdep3.sc.stop();
    }
}
