package hyfd_helper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import hyfd_helper.*;
import scala.Tuple2;

public class DistributedSampler2 implements Serializable{

	private static final long serialVersionUID = 1L;
	private static FDSet negCover;
	private static int numAttributes;
	private static int numberTuples;
	private static float efficiencyThreshold;
	private static int batchSize;
	private static int numPartitions;
	private static HashMap<Integer, ArrayList<Integer>> subPartitionMap = new HashMap<Integer, ArrayList<Integer>>();
	private static JavaPairRDD<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> cartesianPartition = null;
	public static Dataset<Row> df = null;
	public static JavaSparkContext sc;
	private static int posInCartesianPartition = 0;
	
	public DistributedSampler2(FDSet l_negCover, float l_efficiencyThreshold, int l_batchSize, int l_numAttributes, int l_numberTuples, int l_numPartitions, Dataset<Row> l_df, JavaSparkContext l_sc) {
		negCover = l_negCover;
		efficiencyThreshold = l_efficiencyThreshold;
		numAttributes = l_numAttributes;
		numberTuples = l_numberTuples;
		numPartitions = l_numPartitions;
		df = l_df;
		sc = l_sc;
		batchSize = l_batchSize;
		//initComparisonMap();
		partitionData();
	}

	public void initComparisonMap(){
		int key = 0;
		for(int i = 0; i < numPartitions; i++) {
        	for(int j = i; j < numPartitions; j++) {
        		ArrayList<Integer> l = new ArrayList<Integer>();
        		l.add(i);
        		l.add(j);
        		subPartitionMap.put(key++, l);
        	}
        }
	}
	
	/**
     * Random partitioning of data.
     */
    private static void partitionData() {
    	final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
    	
    	JavaRDD<Row> tableRDD = df.javaRDD();
    	JavaPairRDD<Integer, Row> rowMap = tableRDD.mapToPair(
    			new PairFunction<Row, Integer, Row>() {
	    		  public Tuple2<Integer, Row> call(Row r) { 
	    			  return new Tuple2<Integer, Row>((int)(Math.random()*b_numPartitions.value()), r); 
	    			  }
    		});
    	JavaPairRDD<Integer, Iterable<Row>> pairsPartition = rowMap.groupByKey();
    	JavaPairRDD<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> joinedPairs = pairsPartition.cartesian(pairsPartition);
    	joinedPairs = joinedPairs.filter(new Function<Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>>, Boolean> () {
    		public Boolean call(Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> t) {
    			if(t._1._1 > t._2._1)
    				return false;
    			return true;
    		}
    	});
    	// (1: {{1,2,3},{4,5,6}}; 2: {{1,2,3},{7,8,9}})
    	cartesianPartition = joinedPairs.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>>, Iterable<Row>, Iterable<Row>>(){
    		public Tuple2<Iterable<Row>, Iterable<Row>> call(Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> t) {
    			return new Tuple2<Iterable<Row>, Iterable<Row>>(t._1._2, t._2._2);
    		}
    	}).zipWithIndex().mapToPair(new PairFunction<Tuple2<Tuple2<Iterable<Row>, Iterable<Row>>, Long>, Integer, Tuple2<Iterable<Row>, Iterable<Row>>>(){
    		public Tuple2<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> call(Tuple2<Tuple2<Iterable<Row>, Iterable<Row>>, Long> t){
    			return new Tuple2<Integer, Tuple2<Iterable<Row>, Iterable<Row>>>(t._2.intValue(), t._1);
    		}
    	});
    	cartesianPartition.cache();
    	
    	/*// DEBUG
    	Map<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> tmp = cartesianPartition.collectAsMap();
    	for(Integer key: tmp.keySet()) {
    		System.out.println(" ***** "+ key);
    		System.out.println("LEFT:");
    		for(Row r : tmp.get(key)._1)
    			System.out.println(r);
    		System.out.println("RIGHT:");
    		for(Row r : tmp.get(key)._2)
    			System.out.println(r);
    	}*/
    }
	
	public FDList enrichNegativeCover() {
		Logger.getInstance().writeln("Enriching Negative Cover ... ");
		FDList newNonFds = new FDList(numAttributes, negCover.getMaxDepth());
		
		// Replace runNext() with the next run of partitioned batch:
		// eg. 12, 13, 14, 15, 23, 24, 25, 34, 35, 45
		// one batch could be {12, 13} with 2 executors, next batch could be of double the size
		// so, {14, 15, 23, 24}, and so on.
		float efficiency = runNext(newNonFds, negCover, batchSize);
		int i = 1;
		while(efficiency >= efficiencyThreshold) {
			i++;
			batchSize = batchSize*2;
			efficiencyThreshold = efficiencyThreshold/2;
			efficiency = runNext(newNonFds, negCover, batchSize);
		}
		efficiencyThreshold = efficiencyThreshold/2;
		
		return newNonFds;
	}
	
	public float runNext(FDList newNonFds, FDSet negCover, int batchSize) {
		int previousNegCoverSize = newNonFds.size();
		int numNewNonFds = 0;
		long numComparisons = 0;
		
    	final Broadcast<Integer> b_numberAttributes = sc.broadcast(numAttributes);
    	final Broadcast<Integer> b_begin = sc.broadcast(posInCartesianPartition);
    	final Broadcast<Integer> b_end = sc.broadcast(posInCartesianPartition + batchSize);
    	
    	System.out.println(" - - - batchSize:" + batchSize+ " startPos: "+ posInCartesianPartition);
    	JavaPairRDD<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> currCartesianPartition = cartesianPartition.filter(new Function<Tuple2<Integer, Tuple2<Iterable<Row>, Iterable<Row>>>, Boolean>(){
    		public Boolean call(Tuple2<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> t){
    			if(t._1 >= b_begin.value() && t._1 < b_end.value())
    				return true;
    			else 
    				return false;
    		}
    	});
    	
    	/*// DEBUG
    	Map<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> tmp = currCartesianPartition.collectAsMap();
    	for(Integer key: tmp.keySet()) {
    		System.out.println(" ***** "+ key);
    		System.out.println("LEFT:");
    		for(Row r : tmp.get(key)._1)
    			System.out.println(r);
    		System.out.println("RIGHT:");
    		for(Row r : tmp.get(key)._2)
    			System.out.println(r);
    	}*/
    	
    	JavaRDD<HashSet<OpenBitSet>> nonFDsHashetRDD = currCartesianPartition.map(
				new Function<Tuple2<Integer, Tuple2<Iterable<Row>, Iterable<Row>>>, HashSet<OpenBitSet>>() {
					public HashSet<OpenBitSet> call(Tuple2<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> t) {
						HashSet<OpenBitSet> result_l = new HashSet<OpenBitSet>();
    					//FDTree negCoverTree = new FDTree(b_numberAttributes.value());
    					Iterable<Row> rl1 = t._2._1;
    					Iterable<Row> rl2 = t._2._2;
    					for(Row r1 : rl1) {
    						for(Row r2 : rl2) {
    							OpenBitSet equalAttrs = new OpenBitSet(b_numberAttributes.value());
    							for (int i = 0; i < b_numberAttributes.value(); i++) {
    								long val1 = r1.getLong(i);
    					            long val2 = r2.getLong(i);
    								if (val1 >= 0 && val2 >= 0 && val1 == val2) {
    									equalAttrs.set(i);
    								}
    							}
    					        result_l.add(equalAttrs);
    						}
    					}
    					return result_l;
					}
		});
    	
    	JavaRDD<OpenBitSet> nonFDsRDD = nonFDsHashetRDD.flatMap(new FlatMapFunction<HashSet<OpenBitSet>, OpenBitSet>(){
    		public Iterator<OpenBitSet> call(HashSet<OpenBitSet> hs) {
    			return hs.iterator();
    		}
    	}).distinct();
		
    	/*// DEBUG
		System.out.println(" Print NegCover: ");
		List<OpenBitSet> l = nonFDsRDD.collect();
		for(OpenBitSet b : l) {
			for(int i = 0; i < numAttributes; i++)
				if(b.get(i))
					System.out.print(i+" ");
			System.out.println();
		}*/
    	
		for(OpenBitSet nonFD : nonFDsRDD.collect()) {
			if (!negCover.contains(nonFD)) {
				//System.out.println(" Add the above to negCover");
				OpenBitSet equalAttrsCopy = nonFD.clone();
				negCover.add(equalAttrsCopy);
				newNonFds.add(equalAttrsCopy);
			}
		}
		
		posInCartesianPartition = posInCartesianPartition + batchSize;
		int partitionSize = numberTuples/numPartitions;
		numComparisons = (long) partitionSize*partitionSize*batchSize;
		
		numNewNonFds = newNonFds.size() - previousNegCoverSize;
		float efficiency = (float) numNewNonFds/numComparisons;
		
		System.out.println("new NonFDs: "+numNewNonFds+" # comparisons: "+numComparisons+" efficiency: "+efficiency+" efficiency threshold: "+efficiencyThreshold);
		return efficiency;
	}
	
}
