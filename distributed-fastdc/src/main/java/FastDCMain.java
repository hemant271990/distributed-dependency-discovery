
/*import ETL.pipeline;
import ETL.relational.Cell;
import ETL.relational.Sparkdf;
import discovery.dc.SparkFastDC;*/
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import fastdc_impl.*;
import fastdc_helper.*;


import java.util.HashMap;


public class FastDCMain implements Serializable {
    String datapath = "./dataset/Experiments/old/TaxGenerator/inputDB";
    String format = "CSV";
    String header = "true";

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("DistributedFastDC");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		SparkFastDC.jsc = new JavaSparkContext(sparkConf); 
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println("numReducers: "+args[1]);
		System.out.println("sideLen: "+args[2]);
		SparkFastDC.datasetFile = args[0];
		SparkFastDC.numReducers = Integer.parseInt(args[1]);
		SparkFastDC.sideLen = Integer.parseInt(args[2]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastDCMain")
				  .getOrCreate();
		SparkFastDC.df = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true").load(SparkFastDC.datasetFile);
		SparkFastDC.df.printSchema();
		SparkFastDC.columnNames = SparkFastDC.df.columns();
		SparkFastDC.numberAttributes = SparkFastDC.columnNames.length;
		SparkFastDC.df.cache();
		SparkFastDC.numberTuples = SparkFastDC.df.count();
		Long start = System.currentTimeMillis();
		SparkFastDC.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        SparkFastDC.jsc.stop();
        
    }
}