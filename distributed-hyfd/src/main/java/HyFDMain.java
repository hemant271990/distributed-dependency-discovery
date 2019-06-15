import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import hyfd_impl.*;

public class HyFDMain
{
    public static void main( String[] args )
    {
        SparkConf sparkConf = new SparkConf().setAppName("DistributedHybridFD");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		/*HyFD.sc = new JavaSparkContext(sparkConf);
		HyFD.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HybridFDMain")
				  .getOrCreate();
		HyFD.df = spark.read().json(HyFD.datasetFile);
		HyFD.columnNames = HyFD.df.columns();
		HyFD.numberAttributes = HyFD.columnNames.length-1;
		HyFD.execute();
		HyFD.sc.stop();*/
		
		// naive
		/*DistributedHyFD1.sc = new JavaSparkContext(sparkConf);
		DistributedHyFD1.datasetFile = args[0];
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HybridFDMain")
				  .getOrCreate();
		DistributedHyFD1.df = spark.read().json(DistributedHyFD1.datasetFile);
		DistributedHyFD1.columnNames = DistributedHyFD1.df.columns();
		DistributedHyFD1.numberAttributes = DistributedHyFD1.columnNames.length-1;
		DistributedHyFD1.execute();
		DistributedHyFD1.sc.stop();*/
		
		
		// lmPDP
		DistributedHyFD3.sc = new JavaSparkContext(sparkConf);
		DistributedHyFD3.datasetFile = args[0];
		DistributedHyFD3.numPartitions = Integer.parseInt(args[1]);
		DistributedHyFD3.batchSize = Integer.parseInt(args[2]);
		DistributedHyFD3.validationBatchSize = Integer.parseInt(args[3]);
		System.out.println("\n  ======= Starting HyFD =======");
		System.out.println("datasetFile: "+args[0]);
		System.out.println(DistributedHyFD3.sc.sc().applicationId());
		System.out.println("numPartitions: "+args[1]);
		System.out.println("batchSize: "+args[2]);
		System.out.println("validationBatchSize: "+args[3]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HybridFDMain")
				  .getOrCreate();
		DistributedHyFD3.df = spark.read().json(DistributedHyFD3.datasetFile);
		DistributedHyFD3.columnNames = DistributedHyFD3.df.columns();
		DistributedHyFD3.numberAttributes = DistributedHyFD3.columnNames.length-1;
		DistributedHyFD3.execute();
		DistributedHyFD3.sc.stop();
		
		// smPDP
		/*DistributedHyFD4.sc = new JavaSparkContext(sparkConf);
		DistributedHyFD4.datasetFile = args[0];
		DistributedHyFD4.numPartitions = Integer.parseInt(args[1]);
		DistributedHyFD4.batchSize = Integer.parseInt(args[2]);
		DistributedHyFD4.validationBatchSize = Integer.parseInt(args[3]);
		System.out.println("\n  ======= Starting HyFD =======");
		System.out.println("datasetFile: "+args[0]);
		System.out.println("numPartitions: "+args[1]);
		System.out.println("batchSize: "+args[2]);
		System.out.println("validationBatchSize: "+args[3]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HybridFDMain")
				  .getOrCreate();
		DistributedHyFD4.df = spark.read().json(DistributedHyFD4.datasetFile);
		DistributedHyFD4.columnNames = DistributedHyFD4.df.columns();
		DistributedHyFD4.numberAttributes = DistributedHyFD4.columnNames.length-1;
		DistributedHyFD4.execute();
		DistributedHyFD4.sc.stop();*/
    }
}
