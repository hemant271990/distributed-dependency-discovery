import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import hyucc_impl.*;

public class HyUCCMain
{
    public static void main( String[] args )
    {
        SparkConf sparkConf = new SparkConf().setAppName("DistributedHybridFD");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		HyUCC.sc = new JavaSparkContext(sparkConf);
		HyUCC.datasetFile = args[0];
		System.out.println(args[0]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HybridFDMain")
				  .getOrCreate();
		HyUCC.df = spark.read().json(HyUCC.datasetFile);
		HyUCC.columnNames = HyUCC.df.columns();
		HyUCC.numberAttributes = HyUCC.columnNames.length-1;
		HyUCC.execute();
		HyUCC.sc.stop();
		
		/*DistributedHyUCC3.sc = new JavaSparkContext(sparkConf);
		DistributedHyUCC3.datasetFile = args[0];
		DistributedHyUCC3.numPartitions = Integer.parseInt(args[1]);
		DistributedHyUCC3.batchSize = Integer.parseInt(args[2]);
		DistributedHyUCC3.validationBatchSize = Integer.parseInt(args[3]);
		System.out.println("\n  ======= Starting HyFD =======");
		System.out.println(args[0]);
		System.out.println("numPartitions: "+args[1]);
		System.out.println("batchSize: "+args[2]);
		System.out.println("validationBatchSize: "+args[3]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HybridFDMain")
				  .getOrCreate();
		DistributedHyUCC3.df = spark.read().json(DistributedHyUCC3.datasetFile);
		DistributedHyUCC3.columnNames = DistributedHyUCC3.df.columns();
		DistributedHyUCC3.numberAttributes = DistributedHyUCC3.columnNames.length-1;
		DistributedHyUCC3.execute();
		DistributedHyUCC3.sc.stop();*/
    }
}
