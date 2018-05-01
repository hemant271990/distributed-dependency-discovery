package hydra_helper;
/*
 * This is a distributed version of Sampling
 */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import gnu.trove.iterator.TIntIterator;

public class ColumnAwareEvidenceSetBuilder2 extends EvidenceSetBuilder implements Serializable {
	private enum SamplingType {
		WITHIN, LATER, BEFORE, OTHER;
	}
	private static JavaSparkContext sc;
	private Collection<ColumnPair> pairs;
	private static Map<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cartesianPartition = null;
	public static int numPartitions = 55;
	public static int batchSize = 55;
	public static int samplingCount = 1;
	public ColumnAwareEvidenceSetBuilder2(PredicateBuilder predicates2, JavaSparkContext sc2) {
		super(predicates2);
		sc = sc2;
	}
	
	private class SamplingMethod {
		private ColumnData data;
		private SamplingType type;
		private double efficiency = 0.0;

		public SamplingMethod(ColumnData data, SamplingType type) {
			this.data = data;
			this.type = type;
		}

		public void execute(Input input, IEvidenceSet evidenceSet) {
			int sizePrior = evidenceSet.size();
			forEachLine(data.clusters, (clusterIndex, line1) -> {
				PredicateBitSet staticSet = getStatic(pairs, line1);
				switch (type) {
				case BEFORE:
					if (clusterIndex > 0) {
						OrderedCluster randOtherCluster = data.picker.getRandom(0, clusterIndex);
						int line2 = getRandomLine(randOtherCluster);
						evidenceSet.add(getPredicateSet(staticSet, pairs, line1, line2));
					}
					break;
				case LATER:
					if (clusterIndex < data.clusters.size() - 1) {
						OrderedCluster randOtherCluster = data.picker.getRandom(clusterIndex + 1, data.clusters.size());
						int line2 = getRandomLine(randOtherCluster);
						evidenceSet.add(getPredicateSet(staticSet, pairs, line1, line2));
					}
					break;
				case OTHER:
					if (data.clusters.size() > 1) {
						OrderedCluster randOtherCluster = data.picker.getRandom(clusterIndex);
						int line2 = getRandomLine(randOtherCluster);
						evidenceSet.add(getPredicateSet(staticSet, pairs, line1, line2));
					}
					break;
				case WITHIN:
					int line2 = getRandomLine(data.clusters.get(clusterIndex));
					if (line1 != line2)
						evidenceSet.add(getPredicateSet(staticSet, pairs, line1, line2));
					break;
				default:
					break;

				}
			});
			efficiency = (double) (evidenceSet.size() - sizePrior) / input.getLineCount();
		}
	}

	private static class ColumnData {
		public ColumnData(List<OrderedCluster> clusters, WeightedRandomPicker<OrderedCluster> picker, boolean comparable) {
			this.clusters = clusters;
			this.picker = picker;
			this.comparable = comparable;
		}

		public List<OrderedCluster> clusters;
		public WeightedRandomPicker<OrderedCluster> picker;
		public boolean comparable;
	}

	public ColumnAwareEvidenceSetBuilder2(PredicateBuilder predicates2) {
		super(predicates2);
	}

	// HEMANT: Do this for a random partition of the data.
	public IEvidenceSet buildEvidenceSet(IEvidenceSet evidenceSet, Input input, double efficiencyThreshold) {
//		long start = System.currentTimeMillis();
		pairs = predicates.getColumnPairs();
		createSets(pairs);

		List<ColumnData> columnDatas = new ArrayList<>();

		SamplingType[] comparableTypes = { SamplingType.WITHIN, SamplingType.BEFORE, SamplingType.LATER };
		SamplingType[] otherTypes = { SamplingType.WITHIN, SamplingType.OTHER };

		// HEMANT: The priorityQ that keeps track of the efficient sampling methods
		PriorityQueue<SamplingMethod> methods = new PriorityQueue<>(
				(p1, p2) -> Double.compare(p2.efficiency, p1.efficiency));

		for (ParsedColumn<?> c : input.getColumns()) {
			System.out.println("Sampling column " + c.getName());
			Map<Object, OrderedCluster> valueMap = new HashMap<>();
			for (int i = 0; i < input.getLineCount(); ++i) {
				OrderedCluster cluster = valueMap.computeIfAbsent(c.getValue(i), (k) -> new OrderedCluster());
				cluster.add(i);
			}

			List<OrderedCluster> clusters;
			if (c.isComparableType())
				clusters = valueMap.keySet().stream().sorted().map(key -> valueMap.get(key)).collect(Collectors.toList());
			else {
				clusters = new ArrayList<>(valueMap.values());
			}

			WeightedRandomPicker<OrderedCluster> picker = new WeightedRandomPicker<>();
			for (OrderedCluster cluster : clusters) {
				cluster.randomize();
				picker.add(cluster, /*cluster.size()*/1);
			}
			ColumnData d = new ColumnData(clusters, picker, c.isComparableType());
			columnDatas.add(d);

			SamplingType[] types = d.comparable ? comparableTypes : otherTypes;
			for (SamplingType type : types) {
				SamplingMethod method = new SamplingMethod(d, type);
				methods.add(method);
				// comment this out later
				method.execute(input, evidenceSet);
				if (method.efficiency > efficiencyThreshold)
					methods.add(method);
			}
		}

		System.out.println("Initial size of priority list: " + methods.size());
		Map<String, PredicateBitSet[]> mapCopy = new HashMap<String, PredicateBitSet[]>();
		for(ColumnPair cp : map.keySet()) {
			mapCopy.put(cp.toString(), map.get(cp));
		}
		
		Broadcast<Collection<ColumnPair>> b_pairs = sc.broadcast(pairs);
		Broadcast<Map<String, PredicateBitSet[]>> b_mapCopy = sc.broadcast(mapCopy);
		System.out.println(" -- Map size: "+map.size());
		System.out.println(" -- MapCopy size: "+mapCopy.size());
		List<SamplingMethod> sparkBatch = new ArrayList<SamplingMethod>();
		while (!methods.isEmpty()) {
			SamplingMethod method = methods.poll();
			
			sparkBatch.add(method);
			
			if(sparkBatch.size() == 55 || methods.isEmpty()) {
				int sizePrior = evidenceSet.size();
				JavaRDD<SamplingMethod> sparkBatchRDD = sc.parallelize(sparkBatch, 55);
				JavaRDD<Tuple2<SamplingMethod, List<PredicateBitSet>>> predicatesResultRDD = sparkBatchRDD.map(new Function<SamplingMethod, Tuple2<SamplingMethod, List<PredicateBitSet>>>() {
					public Tuple2<SamplingMethod, List<PredicateBitSet>> call(SamplingMethod m) {
						//m.execute(input, evidenceSet);
						
						//int sizePrior = evidenceSet.size();
						List<PredicateBitSet> currEvidenceSet = new ArrayList<PredicateBitSet>();
						int clusterIndex = 0;
						for (OrderedCluster cluster : m.data.clusters) {
							TIntIterator iter = cluster.iterator();
							while (iter.hasNext()) {
								int line1 = iter.next();
								//consumer.accept(clusterIndex, line1);

								PredicateBitSet staticSet = getStaticSpark(b_pairs.value(), line1, b_mapCopy.value());
								switch (m.type) {
								case BEFORE:
									if (clusterIndex > 0) {
										OrderedCluster randOtherCluster = m.data.picker.getRandom(0, clusterIndex);
										int line2 = getRandomLine(randOtherCluster);
										currEvidenceSet.add(getPredicateSetSpark(staticSet, b_pairs.value(), line1, line2, b_mapCopy.value()));
									}
									break;
								case LATER:
									if (clusterIndex < m.data.clusters.size() - 1) {
										OrderedCluster randOtherCluster = m.data.picker.getRandom(clusterIndex + 1, m.data.clusters.size());
										int line2 = getRandomLine(randOtherCluster);
										currEvidenceSet.add(getPredicateSetSpark(staticSet, b_pairs.value(), line1, line2, b_mapCopy.value()));
									}
									break;
								case OTHER:
									if (m.data.clusters.size() > 1) {
										OrderedCluster randOtherCluster = m.data.picker.getRandom(clusterIndex);
										int line2 = getRandomLine(randOtherCluster);
										currEvidenceSet.add(getPredicateSetSpark(staticSet, b_pairs.value(), line1, line2, b_mapCopy.value()));
									}
									break;
								case WITHIN:
									int line2 = getRandomLine(m.data.clusters.get(clusterIndex));
									if (line1 != line2)
										currEvidenceSet.add(getPredicateSetSpark(staticSet, b_pairs.value(), line1, line2, b_mapCopy.value()));
									break;
								default:
									break;
								}
							}
							++clusterIndex;
						}
						
						//forEachLine(data.clusters, (clusterIndex, line1) -> {});
						//double currEfficiency = (double) (currEvidenceSet.size() - b_sizePrior.value()) / input.getLineCount();
						return new Tuple2<SamplingMethod, List<PredicateBitSet>>(m, currEvidenceSet);
					}
				});
				
				for(Tuple2<SamplingMethod, List<PredicateBitSet>> e : predicatesResultRDD.collect()) {
					sizePrior = evidenceSet.size();
					for(PredicateBitSet bs : e._2)
						evidenceSet.add(bs);
					double currEfficiency = (double) (evidenceSet.size() - sizePrior) / input.getLineCount();
					e._1.efficiency = currEfficiency;
					//System.out.println("# new evidences: " + (evidenceSet.size() - sizePrior));
					//System.out.println("Efficiency: " + e._1.efficiency);
					if (currEfficiency > efficiencyThreshold)
						methods.add(e._1);
				}
				sparkBatch = new ArrayList<SamplingMethod>();
			} else
				continue;
			/*method.execute(input, evidenceSet);
			if (method.efficiency > efficiencyThreshold)
				methods.add(method);*/
		}

		return evidenceSet;
	}
	
	public IEvidenceSet buildEvidenceSetUniformPartition(IEvidenceSet evidenceSet, Input input, double efficiencyThreshold, Dataset<Row> df) {
		long t1 = System.currentTimeMillis();
		partitionData(df);
		/*Collection<ColumnPair> pairs = predicates.getColumnPairs();
		createSets(pairs);*/
		
		pairs = predicates.getColumnPairs();
		createSets(pairs);

		//IEvidenceSet evidenceSet = new TroveEvidenceSet();
		int posInCartesianPartition = 0;
		Map<String, PredicateBitSet[]> mapCopy = new HashMap<String, PredicateBitSet[]>();
		for(ColumnPair cp : map.keySet()) {
			mapCopy.put(cp.toString(), map.get(cp));
		}
		Broadcast<Collection<ColumnPair>> b_pairs = sc.broadcast(pairs);
		Broadcast<Map<String, PredicateBitSet[]>> b_mapCopy = sc.broadcast(mapCopy);
		long t2 = System.currentTimeMillis();
		System.out.println(" --Partition + Broadcast time "+ (t2-t1)/1000);
		
		
		List<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>> currentMap = new ArrayList<Tuple2<Integer, Tuple2< Iterable<Integer>, Iterable<Integer>>>>();
		for(int j = 0; j < samplingCount; j++) {
			System.out.println("* * * sampling count: "+j);
			for(int i = posInCartesianPartition; i < posInCartesianPartition + batchSize; i++) {
	    		if(i >= cartesianPartition.size())
	    			break;
	    		currentMap.add(new Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>(i, cartesianPartition.get(i)));
	    	}
			
			JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> currentJobRDD = sc.parallelizePairs(currentMap, numPartitions);
			List<Set<PredicateBitSet>> evidencesResult = currentJobRDD.map(new Function<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>, Set<PredicateBitSet>>(){
				public Set<PredicateBitSet> call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> t) {
					Set<PredicateBitSet> result = new HashSet<PredicateBitSet>();
					for(Integer i : t._2._1){
						PredicateBitSet staticSet = getStaticSpark(b_pairs.value(), i, b_mapCopy.value());
						for(Integer j : t._2._2) {
							if(i == j)
								continue;
							PredicateBitSet set = getPredicateSetSpark(staticSet, b_pairs.value(), i, j, b_mapCopy.value());
							result.add(set);
						}
					}
					
					return result;
				}
			}).collect();
			
			for(Set<PredicateBitSet> l : evidencesResult) {
				//System.out.println("-- Result size: "+l.size());
				for(PredicateBitSet bs : l) {
					//System.out.println("-- Result: "+bs.toString());
					evidenceSet.add(bs);
				}
			}
			
			posInCartesianPartition = posInCartesianPartition + batchSize;
			currentMap = new ArrayList<Tuple2<Integer, Tuple2< Iterable<Integer>, Iterable<Integer>>>>();
		}
		long t3 = System.currentTimeMillis();
		System.out.println(" --EVSet generation time "+ (t3-t2)/1000);
		
		return evidenceSet;
	}
	
	/**
     * Random partitioning of data.
     */
    private static void partitionData(Dataset<Row> df) {
    	
    	final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
    	
    	//JavaRDD<Row> tableRDD = df.javaRDD();
    	List<Integer> pairingsList = new ArrayList<Integer>();
		
		long numTuples = df.count();
		for (int i = 0; i < numTuples; ++i) {
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

	private static void forEachLine(List<OrderedCluster> clusters, BiConsumer<Integer, Integer> consumer) {
		int clusterIndex = 0;
		for (OrderedCluster cluster : clusters) {
			TIntIterator iter = cluster.iterator();
			while (iter.hasNext()) {
				int line1 = iter.next();
				consumer.accept(clusterIndex, line1);
			}
			++clusterIndex;
		}
	}

	private int getRandomLine(OrderedCluster c) {
		return c.nextLine();
	}

	private static Logger log = LoggerFactory.getLogger(ColumnAwareEvidenceSetBuilder2.class);
}
