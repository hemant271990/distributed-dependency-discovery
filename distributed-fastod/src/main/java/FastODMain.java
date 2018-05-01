
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import fastod_impl.*;

/**
 * Created by Mehdi on 6/20/2016.
 */
public class FastODMain {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("DistributedFastOD");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		/*ODAlgorithm.sc = new JavaSparkContext(sparkConf); 
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println("batch size: "+args[1]);
		ODAlgorithm.DatasetFileName = args[0];
		ODAlgorithm.batch_size = Integer.parseInt(args[1]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastODMain")
				  .getOrCreate();
		ODAlgorithm.df = spark.read().json(ODAlgorithm.DatasetFileName);
		ODAlgorithm.df.printSchema();
		ODAlgorithm.columnNames = ODAlgorithm.df.columns();
		ODAlgorithm.numberAttributes = ODAlgorithm.columnNames.length-1;
		ODAlgorithm.df.cache();
		ODAlgorithm.numberTuples = ODAlgorithm.df.count();
		Long start = System.currentTimeMillis();
		ODAlgorithm.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        ODAlgorithm.sc.stop();*/
		
		/*DistributedFastOD1.sc = new JavaSparkContext(sparkConf); 
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println("batch size: "+args[1]);
		DistributedFastOD1.DatasetFileName = args[0];
		DistributedFastOD1.batch_size = Integer.parseInt(args[1]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastODMain")
				  .getOrCreate();
		DistributedFastOD1.df = spark.read().json(DistributedFastOD1.DatasetFileName);
		DistributedFastOD1.df.printSchema();
		DistributedFastOD1.columnNames = DistributedFastOD1.df.columns();
		DistributedFastOD1.numberAttributes = DistributedFastOD1.columnNames.length-1;
		DistributedFastOD1.df.cache();
		DistributedFastOD1.numberTuples = DistributedFastOD1.df.count();
		Long start = System.currentTimeMillis();
		DistributedFastOD1.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        DistributedFastOD1.sc.stop();*/
        
        DistributedFastOD2.sc = new JavaSparkContext(sparkConf); 
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println("batch size: "+args[1]);
		DistributedFastOD2.DatasetFileName = args[0];
		DistributedFastOD2.batch_size = Integer.parseInt(args[1]);
		SparkSession spark = SparkSession
				  .builder()
				  .appName("FastODMain")
				  .getOrCreate();
		DistributedFastOD2.df = spark.read().json(DistributedFastOD2.DatasetFileName);
		DistributedFastOD2.df.printSchema();
		DistributedFastOD2.columnNames = DistributedFastOD2.df.columns();
		DistributedFastOD2.numberAttributes = DistributedFastOD2.columnNames.length-1;
		DistributedFastOD2.df.cache();
		DistributedFastOD2.numberTuples = DistributedFastOD2.df.count();
		Long start = System.currentTimeMillis();
		DistributedFastOD2.execute();
        Long end = System.currentTimeMillis();
        System.out.println(" ==== Total: " + (end - start)/1000 + " sec");
        DistributedFastOD2.sc.stop();
	}

    /*public static void main(String[] args) {

        if(FindUnionBool) {
            findUnion();
            return;
        }
        printTime();

        //TANE      FastOD      ORDER
        //Comma     Tab
        try {
            BufferedReader br = new BufferedReader(new FileReader(ConfigFileName));

            DatasetFileName = br.readLine().trim();
            AlgorithmName = br.readLine().trim();

            MaxRowNumber = Integer.parseInt(br.readLine().trim());
            MaxColumnNumber = Integer.parseInt(br.readLine().trim());
            RunTimeNumber = Integer.parseInt(br.readLine().trim());

            String lineSeparator = br.readLine().trim();
            if(lineSeparator.equals("Comma"))
                cvsSplitBy = ",";
            if(lineSeparator.equals("Tab"))
                cvsSplitBy = "\t";

            String pruneS = br.readLine().trim();
            if(pruneS.equals("PruneFalse"))
                Prune = false;

            String InterestingnessPruneS = br.readLine().trim();
            if(InterestingnessPruneS.equals("InterestingnessPruneTrue"))
                InterestingnessPrune = true;

            InterestingnessThreshold = Long.parseLong(br.readLine().trim());

            topk = Integer.parseInt(br.readLine().trim());

            String BidirectionalTrueS = br.readLine().trim();
            if(BidirectionalTrueS.equals("BidirectionalTrue"))
                BidirectionalTrue = true;

            String RankDoubleTrueS = br.readLine().trim();
            if(!RankDoubleTrueS.equals("RankDoubleTrue"))
                RankDoubleTrue = false;

            String ReverseRankingTrueS = br.readLine().trim();
            if(ReverseRankingTrueS.equals("ReverseRankingTrue"))
                ReverseRankingTrue = true;

            String BidirectionalPruneTrueS = br.readLine().trim();
            if(BidirectionalPruneTrueS.equals("BidirectionalPruneTrue"))
                BidirectionalPruneTrue = true;

            ReverseRankingPercentage = Integer.parseInt(br.readLine().trim());

        }catch (Exception ex){
            ex.printStackTrace();
            return;
        }

        TaneAlgorithm taneAlgorithm = new TaneAlgorithm();

        ODAlgorithm ODAlgorithm = new ODAlgorithm();


        ORDERLhsRhs ORDERAlgorithm = new ORDERLhsRhs();

        System.out.println("Algorithm: " + AlgorithmName);
        System.out.println("InterestingnessPrune: " + InterestingnessPrune);
        System.out.println("InterestingnessThreshold: " + InterestingnessThreshold);
        System.out.println("BidirectionalTrue: " + BidirectionalTrue);
        System.out.println("BidirectionalPruneTrue: " + BidirectionalPruneTrue);

        try {
            long startTime = System.currentTimeMillis();

            for(int i=0; i<RunTimeNumber; i ++) {

                if (AlgorithmName.equals("TANE"))
                    taneAlgorithm.execute();

                if (AlgorithmName.equals("FastOD"))
                    ODAlgorithm.execute();

                if (AlgorithmName.equals("ORDER"))
                    ORDERAlgorithm.execute();

                FirstTimeRun = false;
            }

            long endTime = System.currentTimeMillis();
            long runTime = (endTime - startTime)/RunTimeNumber;

            System.out.println("Run Time (ms): " + runTime);
        }catch(Exception ex){
            System.out.println("Error");
            ex.printStackTrace();
        }

        printTime();

        //print results to a file
        try {
            BufferedWriter bw =
                    new BufferedWriter(new FileWriter("D:/Code/Datasets/OD/unionN.txt"));

            for(String str : odList)
                bw.write(str + "\n");

            bw.close();
        }catch(Exception ex){

        }

    }*/

    public static void printTime(){
        Calendar now = Calendar.getInstance();
        int year = now.get(Calendar.YEAR);
        int month = now.get(Calendar.MONTH) + 1; // Note: zero based!
        int day = now.get(Calendar.DAY_OF_MONTH);
        int hour = now.get(Calendar.HOUR_OF_DAY);
        int minute = now.get(Calendar.MINUTE);
        int second = now.get(Calendar.SECOND);
        int millis = now.get(Calendar.MILLISECOND);

        System.out.printf("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        System.out.println("\n");
    }

    public static void printOpenBitSet(OpenBitSet bitSet, int maxLength){
        for(int i=0; i<maxLength; i ++){
            if(bitSet.get(i))
                System.out.print(1 + " ");
            else
                System.out.print(0 + " ");
        }
        System.out.println("");
    }

    public static void findUnion(){
        try {
            BufferedReader br = new BufferedReader(new FileReader("D:/Code/Datasets/OD/union.txt"));

            Set<String> union = new HashSet<String>();
            String str = null;
            while((str = br.readLine()) != null){
                union.add(str);
            }

            System.out.println("Union Size: " + union.size());

            br.close();
        }catch(Exception ex){

        }
    }
}

