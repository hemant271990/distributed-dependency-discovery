import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import hydra_helper.DenialConstraintSet;
import hydra_impl.*;

public class HydraMain
{
    public static void main( String[] args )
    {
        SparkConf sparkConf = new SparkConf().setAppName("DistributedHydra");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		Hydra hydra = new Hydra();
		hydra.sc =  new JavaSparkContext(sparkConf);
		String datasetFile = args[0];
		System.out.println(args[0]);
		System.out.println("numPartitions: "+args[1]);
		System.out.println("samplingCount: "+args[2]);
		Hydra.numPartitions = Integer.parseInt(args[1]);
		Hydra.samplingCount = Integer.parseInt(args[2]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("HydraMain")
				  .getOrCreate();
		long t1 = System.currentTimeMillis();
		hydra.df = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true").load(datasetFile);
					//spark.read().csv(datasetFile);
		DenialConstraintSet result = hydra.execute();
		long t2 = System.currentTimeMillis();
		System.out.println("==== FINAL RESULT: "+result.size()+" DCs");
		System.out.println("==== TOTAL TIME: "+ (t2-t1)/1000 + "\n");
		hydra.sc.stop();
				
    }
}
