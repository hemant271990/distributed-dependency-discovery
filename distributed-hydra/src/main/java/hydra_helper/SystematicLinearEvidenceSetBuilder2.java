package hydra_helper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/*import de.hpi.naumann.dc.evidenceset.IEvidenceSet;
import de.hpi.naumann.dc.evidenceset.TroveEvidenceSet;
import de.hpi.naumann.dc.evidenceset.build.EvidenceSetBuilder;
import de.hpi.naumann.dc.input.ColumnPair;
import de.hpi.naumann.dc.input.Input;
import de.hpi.naumann.dc.predicates.PredicateBuilder;
import de.hpi.naumann.dc.predicates.sets.PredicateBitSet;*/
import hydra_helper.*;
import scala.Tuple2;

public class SystematicLinearEvidenceSetBuilder2 extends EvidenceSetBuilder implements Serializable {
	private final int factor;
	private static JavaSparkContext sc;
	private static Map<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cartesianPartition = null;
	public static int numPartitions = 55;
	public static int batchSize = 55;
	public static int samplingCount = 10;
	
	public SystematicLinearEvidenceSetBuilder2(PredicateBuilder pred, int factor, JavaSparkContext sc2) {
		super(pred);
		this.factor = factor;
		sc = sc2;
	}

	public IEvidenceSet buildEvidenceSet(Input input) {
		IEvidenceSet evidenceSet = new TroveEvidenceSet();
		
		Collection<ColumnPair> pairs = predicates.getColumnPairs();
		createSets(pairs);

		List<Tuple2<Integer, List<Integer>>> pairingsList = new ArrayList<Tuple2<Integer, List<Integer>>>();
		
		Random r = new Random();
		for (int i = 0; i < input.getLineCount(); ++i) {
			List<Integer> innerList = new ArrayList<Integer>();
			for (int count = 0; count < factor; ++count) {
				int j = r.nextInt(input.getLineCount() - 1);
				if (j >= i)
					j++;
				innerList.add(j);
			}
			pairingsList.add(new Tuple2<Integer, List<Integer>>(i, innerList));
		}
		System.out.println("Pairings list size: "+ pairingsList.size());
		
		Map<String, PredicateBitSet[]> mapCopy = new HashMap<String, PredicateBitSet[]>();
		for(ColumnPair cp : map.keySet()) {
			mapCopy.put(cp.toString(), map.get(cp));
		}
		
		Broadcast<Collection<ColumnPair>> b_pairs = sc.broadcast(pairs);
		Broadcast<Map<String, PredicateBitSet[]>> b_mapCopy = sc.broadcast(mapCopy);
		JavaPairRDD<Integer, List<Integer>> pairingsRDD = sc.parallelizePairs(pairingsList);
		List<List<PredicateBitSet>> evidencesResult = pairingsRDD.map(new Function<Tuple2<Integer, List<Integer>>, List<PredicateBitSet>>(){
			public List<PredicateBitSet> call(Tuple2<Integer, List<Integer>> t) {
				List<PredicateBitSet> result = new ArrayList<PredicateBitSet>();
				PredicateBitSet staticSet = getStaticSpark(b_pairs.value(), t._1, b_mapCopy.value());
				for(int j : t._2) {
					PredicateBitSet set = getPredicateSetSpark(staticSet, b_pairs.value(), t._1, j, b_mapCopy.value());
					result.add(set);
				}
				return result;
			}
		}).collect();
		
		for(List<PredicateBitSet> l : evidencesResult) {
			for(PredicateBitSet bs : l) {
				//System.out.println("-- Result: "+bs.toString());
				evidenceSet.add(bs);
			}
		}
		
		return evidenceSet;
	}
	
	public IEvidenceSet buildEvidenceSetUniformPartition(IEvidenceSet evidenceSet, Dataset<Row> df) {
		partitionData(df);
		Collection<ColumnPair> pairs = predicates.getColumnPairs();
		createSets(pairs);

		//IEvidenceSet evidenceSet = new TroveEvidenceSet();
		int posInCartesianPartition = 0;
		Map<String, PredicateBitSet[]> mapCopy = new HashMap<String, PredicateBitSet[]>();
		for(ColumnPair cp : map.keySet()) {
			mapCopy.put(cp.toString(), map.get(cp));
		}
		Broadcast<Collection<ColumnPair>> b_pairs = sc.broadcast(pairs);
		Broadcast<Map<String, PredicateBitSet[]>> b_mapCopy = sc.broadcast(mapCopy);
		
		List<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>> currentMap = new ArrayList<Tuple2<Integer, Tuple2< Iterable<Integer>, Iterable<Integer>>>>();
		for(int j = 0; j < samplingCount; j++) {
			for(int i = posInCartesianPartition; i < posInCartesianPartition + batchSize; i++) {
	    		if(i >= cartesianPartition.size())
	    			break;
	    		currentMap.add(new Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>(i, cartesianPartition.get(i)));
	    	}
			
			JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> currentJobRDD = sc.parallelizePairs(currentMap, numPartitions);
			List<List<PredicateBitSet>> evidencesResult = currentJobRDD.map(new Function<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>, List<PredicateBitSet>>(){
				public List<PredicateBitSet> call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> t) {
					List<PredicateBitSet> result = new ArrayList<PredicateBitSet>();
					for(Integer i : t._2._1){
						PredicateBitSet staticSet = getStaticSpark(b_pairs.value(), i, b_mapCopy.value());
						for(Integer j : t._2._2) {
							PredicateBitSet set = getPredicateSetSpark(staticSet, b_pairs.value(), i, j, b_mapCopy.value());
							result.add(set);
						}
					}
					
					return result;
				}
			}).collect();
			
			for(List<PredicateBitSet> l : evidencesResult) {
				for(PredicateBitSet bs : l) {
					System.out.println("-- Result: "+bs.toString());
					evidenceSet.add(bs);
				}
			}
			
			posInCartesianPartition = posInCartesianPartition + batchSize;
			currentMap = new ArrayList<Tuple2<Integer, Tuple2< Iterable<Integer>, Iterable<Integer>>>>();
		}
		
		return evidenceSet;
	}
	
	protected PredicateBitSet getStaticSpark(Collection<ColumnPair> pairs, int i, Map<String, PredicateBitSet[]> b_map) {
		//PredicateBitSet set = PredicateSetFactory.create();
		PredicateBitSet set = new PredicateBitSet();
		// which predicates are satisfied by these two lines?
		for (ColumnPair p : pairs) {
			if (p.getC1().equals(p.getC2()))
				continue;

			PredicateBitSet[] list = b_map.get(p.toString());
			if (p.isJoinable()) {
				if (equals(i, i, p))
					set.addAll(list[2]);
				else
					set.addAll(list[3]);
			}
			if (p.isComparable()) {
				int compare2 = compare(i, i, p);
				if (compare2 < 0) {
					set.addAll(list[7]);
				} else if (compare2 == 0) {
					set.addAll(list[8]);
				} else {
					set.addAll(list[9]);
				}
			}

		}
		return set;
	}
	
	protected PredicateBitSet getPredicateSetSpark(PredicateBitSet staticSet, Collection<ColumnPair> pairs, int i, int j, Map<String, PredicateBitSet[]> b_map) {
		//PredicateBitSet set = PredicateSetFactory.create(staticSet);
		PredicateBitSet set = new PredicateBitSet(staticSet);
		// which predicates are satisfied by these two lines?
		for (ColumnPair p : pairs) {
			PredicateBitSet[] list = b_map.get(p.toString());
			//System.out.println(p);
			if (p.isJoinable()) {
				if (equals(i, j, p))
					set.addAll(list[0]);
				else
					set.addAll(list[1]);
			}
			if (p.isComparable()) {
				int compare = compare(i, j, p);
				if (compare < 0) {
					set.addAll(list[4]);
				} else if (compare == 0) {
					set.addAll(list[5]);
				} else {
					set.addAll(list[6]);
				}

			}

		}
		return set;
	}
	
	/**
     * Random partitioning of data.
     */
    private static void partitionData(Dataset<Row> df) {
    	
    	final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
    	
    	//JavaRDD<Row> tableRDD = df.javaRDD();
    	List<Integer> pairingsList = new ArrayList<Integer>();
		
		Random r = new Random();
		for (int i = 0; i < df.count(); ++i) {
			pairingsList.add(i);
		}
		JavaRDD<Integer> pairingsRDD = sc.parallelize(pairingsList);
    	JavaPairRDD<Integer, Integer> rowMap = pairingsRDD.mapToPair(
    			new PairFunction<Integer, Integer, Integer>() {
	    		  public Tuple2<Integer, Integer> call(Integer r) { 
	    			  return new Tuple2<Integer, Integer>((int)(Math.random()*b_numPartitions.value()), r); 
	    			  }
    		});
    	JavaPairRDD<Integer, Iterable<Integer>> pairsPartition = rowMap.groupByKey();
    	JavaPairRDD<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>> joinedPairs = pairsPartition.cartesian(pairsPartition);
    	joinedPairs = joinedPairs.filter(new Function<Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>>, Boolean> () {
    		public Boolean call(Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>> t) {
    			if(t._1._1 > t._2._1)
    				return false;
    			return true;
    		}
    	});
    	// (1: {{1,2,3},{4,5,6}}; 2: {{1,2,3},{7,8,9}})
    	cartesianPartition = joinedPairs.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>>, Iterable<Integer>, Iterable<Integer>>(){
    		public Tuple2<Iterable<Integer>, Iterable<Integer>> call(Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>> t) {
    			return new Tuple2<Iterable<Integer>, Iterable<Integer>>(t._1._2, t._2._2);
    		}
    	}).zipWithIndex().mapToPair(new PairFunction<Tuple2<Tuple2<Iterable<Integer>, Iterable<Integer>>, Long>, Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>(){
    		public Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> call(Tuple2<Tuple2<Iterable<Integer>, Iterable<Integer>>, Long> t){
    			return new Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>(t._2.intValue(), t._1);
    		}
    	}).collectAsMap();
    }

}
