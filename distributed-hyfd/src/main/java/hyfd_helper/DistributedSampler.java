package hyfd_helper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.primitives.Ints;

import hyfd_helper.*;
import hyfd_impl.DistributedHyFD1;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import scala.Tuple2;

public class DistributedSampler implements Serializable{

	private static final long serialVersionUID = 1L;
	private static FDSet negCover;
	//private List<PositionListIndex> plis;
	private static int numAttributes;
	private static int numberTuples;
	private static float efficiencyThreshold;
	public static int numPartitions = 55;
	private static ValueComparator valueComparator;
	private static List<DistributedAttributeRepresentant> attributeRepresentants = null;
	private static PriorityQueue<DistributedAttributeRepresentant> queue = null;
	private static MemoryGuardian memoryGuardian;

	public DistributedSampler(FDSet l_negCover, float l_efficiencyThreshold, ValueComparator l_valueComparator, MemoryGuardian l_memoryGuardian, int l_numAttributes, int l_numberTuples) {
		negCover = l_negCover;
		//this.plis = plis;
		efficiencyThreshold = l_efficiencyThreshold;
		valueComparator = l_valueComparator;
		memoryGuardian = l_memoryGuardian;
		numAttributes = l_numAttributes;
		numberTuples = l_numberTuples;
	}

	public FDList enrichNegativeCover(List<IntegerPair> comparisonSuggestions, JavaPairRDD<Integer, Integer[]> compressedRecordsRDD, JavaRDD<PositionListIndex> plisRDD, JavaSparkContext sc) {
		//int numAttributes = numAttributes;
		
		Logger.getInstance().writeln("Investigating comparison suggestions ... ");
		FDList newNonFds = new FDList(numAttributes, negCover.getMaxDepth());
		OpenBitSet equalAttrs = new OpenBitSet(numAttributes);
		for (IntegerPair comparisonSuggestion : comparisonSuggestions) {
			// Use RDD filter operation to pick two rows and match them.
			final Broadcast<Integer> b_one = sc.broadcast(comparisonSuggestion.a());
			final Broadcast<Integer> b_two = sc.broadcast(comparisonSuggestion.b());
			
			Map<Integer, Integer[]> comparePairMap = filterRecords(compressedRecordsRDD, b_one, b_two);
			//this.match(equalAttrs, comparisonSuggestion.a(), comparisonSuggestion.b(), compressedRecords);
			this.match(equalAttrs, comparePairMap.get(comparisonSuggestion.a()), comparePairMap.get(comparisonSuggestion.b()));
			
			if (!negCover.contains(equalAttrs)) {
				OpenBitSet equalAttrsCopy = equalAttrs.clone();
				negCover.add(equalAttrsCopy);
				newNonFds.add(equalAttrsCopy);
				
				//memoryGuardian.memoryChanged(1);
				//memoryGuardian.match(negCover, posCover, newNonFds);
			}
		}
		
		final Broadcast<Integer> b_numberTuples = sc.broadcast(numberTuples);
		if (attributeRepresentants == null) { // if this is the first call of this method
			Logger.getInstance().write("Sorting clusters ...");
			long time = System.currentTimeMillis();
			// The bottom line for sorting is that, the neighborhood of each record is different in each PLI.
			// Use random-ness to have different neighborhoods for records. Sort them based on some value b/w numberTuples
			/*JavaRDD<PositionListIndex> sortedPlisRDD = plisRDD.map(new Function<PositionListIndex, PositionListIndex>() {
				public PositionListIndex call(PositionListIndex pli) {
					for (IntArrayList cluster : pli.getClusters()) {
						Collections.sort(cluster, new Comparator<Integer>() {
							public int compare(Integer o1, Integer o2) {
								int value1 = (int) (Math.random()*b_numberTuples.value());
								int value2 = (int) (Math.random()*b_numberTuples.value());
								return value2 - value1;
							}
						});
					}
					return pli;
				}
			});*/
			
			Logger.getInstance().writeln("(" + (System.currentTimeMillis() - time) + "ms)");
		
			Logger.getInstance().write("Running initial windows ...");
			time = System.currentTimeMillis();
			attributeRepresentants = new ArrayList<DistributedAttributeRepresentant>(numAttributes);
			queue = new PriorityQueue<DistributedAttributeRepresentant>(numAttributes);
			// Get one pli at a time at the driver, and do runNext on it.
			for (int i = 0; i < numAttributes; i++) {
				final Broadcast<Integer> b_requiredPliAttr = sc.broadcast(i);
				List<IntArrayList> clusters = filterPlis(plisRDD, b_requiredPliAttr);
				//System.out.println(" Selected PLI num clusters: " + clusters.size());
				
				DistributedAttributeRepresentant attributeRepresentant = new DistributedAttributeRepresentant(this, memoryGuardian, i, numberTuples);
				attributeRepresentant.runNext(newNonFds, compressedRecordsRDD, clusters, sc, negCover);
				//System.out.println(" ##### window distance: "+attributeRepresentant.windowDistance);
				System.out.println(" ##### efficiency for attr: "+i+": "+attributeRepresentant.getEfficiency());
				attributeRepresentants.add(attributeRepresentant);
				if (attributeRepresentant.getEfficiency() > 0.0f)
					queue.add(attributeRepresentant); // If the efficiency is 0, the algorithm will never schedule a next run for the attribute regardless how low we set the efficiency threshold
			}
			
			if (!queue.isEmpty())
				efficiencyThreshold = Math.min(0.01f, queue.peek().getEfficiency() * 0.5f); // This is an optimization that we added after writing the HyFD paper
			
			Logger.getInstance().writeln("(" + (System.currentTimeMillis() - time) + "ms)");
		}
		else {
			// Decrease the efficiency threshold
			if (!queue.isEmpty())
				efficiencyThreshold = Math.min(efficiencyThreshold / 2, queue.peek().getEfficiency() * 0.9f); // This is an optimization that we added after writing the HyFD paper
		}
		
		Logger.getInstance().writeln("Moving window over clusters ... ");
		
		while (!queue.isEmpty() && (queue.peek().getEfficiency() >= efficiencyThreshold)) {
			DistributedAttributeRepresentant attributeRepresentant = queue.remove();
			final Broadcast<Integer> b_requiredPliAttr = sc.broadcast(attributeRepresentant.attribute);
			List<IntArrayList> clusters = filterPlis(plisRDD, b_requiredPliAttr);
			
			attributeRepresentant.runNext(newNonFds, compressedRecordsRDD, clusters, sc, negCover);
			System.out.println(" ##### efficiency for attr: "+attributeRepresentant.attribute+": "+attributeRepresentant.getEfficiency());
			if (attributeRepresentant.getEfficiency() > 0.0f)
				queue.add(attributeRepresentant);
		}
		
		StringBuilder windows = new StringBuilder("Window signature: ");
		for (DistributedAttributeRepresentant attributeRepresentant : attributeRepresentants)
			windows.append("[" + attributeRepresentant.windowDistance + "]");
		Logger.getInstance().writeln(windows.toString());
			
		return newNonFds;
	}
	
	private Map<Integer, Integer[]> filterRecords( JavaPairRDD<Integer, Integer[]> compressedRecordsRDD, final Broadcast<Integer> b_one, final Broadcast<Integer> b_two) {
		JavaPairRDD<Integer, Integer[]> comparePairRDD = compressedRecordsRDD.filter(new Function<Tuple2<Integer, Integer[]>, Boolean>() {
			public Boolean call(Tuple2<Integer, Integer[]> t) {
				if(t._1.equals(b_one.value()) || t._1.equals(b_two.value()))
					return true;
				else
					return false;
			}
		});
		Map<Integer, Integer[]> comparePairMap = comparePairRDD.collectAsMap();
		return comparePairMap;
	}
	
	private List<IntArrayList> filterPlis(JavaRDD<PositionListIndex> plisRDD, final Broadcast<Integer> b_requiredPliAttr) {
		JavaRDD<PositionListIndex> singlePliRDD= plisRDD.filter(new Function<PositionListIndex, Boolean>() {
			public Boolean call(PositionListIndex pli) {
				if(pli.attribute == b_requiredPliAttr.value().intValue())
					return true;
				else
					return false;
			}
		});
		JavaRDD<IntArrayList> clustersRDD = singlePliRDD.flatMap(new FlatMapFunction<PositionListIndex, IntArrayList>() {
			public Iterator<IntArrayList> call(PositionListIndex pli) {
				return pli.getClusters().iterator();
			}
		});
		return clustersRDD.collect();
	}

	private class ClusterComparator implements Comparator<Integer> {
		
		private Map<Integer, Integer[]> sortKeys;
		private int activeKey1;
		private int activeKey2;
		
		public ClusterComparator(Map<Integer, Integer[]> sortKeys, int activeKey1, int activeKey2) {
			super();
			this.sortKeys = sortKeys;
			this.activeKey1 = activeKey1;
			this.activeKey2 = activeKey2;
		}
		
		public void incrementActiveKey() {
			this.activeKey1 = this.increment(this.activeKey1);
			this.activeKey2 = this.increment(this.activeKey2);
		}
		
		@Override
		public int compare(Integer o1, Integer o2) {
			// Next
		/*	int value1 = this.sortKeys[o1.intValue()][this.activeKey2];
			int value2 = this.sortKeys[o2.intValue()][this.activeKey2];
			return value2 - value1;
		*/	
			// Previous
		/*	int value1 = this.sortKeys[o1.intValue()][this.activeKey1];
			int value2 = this.sortKeys[o2.intValue()][this.activeKey1];
			return value2 - value1;
		*/	
			// Previous -> Next
			int value1 = this.sortKeys.get(o1.intValue())[this.activeKey1];
			int value2 = this.sortKeys.get(o2.intValue())[this.activeKey1];
			int result = value2 - value1;
			if (result == 0) {
				value1 = this.sortKeys.get(o1.intValue())[this.activeKey2];
				value2 = this.sortKeys.get(o2.intValue())[this.activeKey2];
			}
			return value2 - value1;
			
			// Next -> Previous
		/*	int value1 = this.sortKeys[o1.intValue()][this.activeKey2];
			int value2 = this.sortKeys[o2.intValue()][this.activeKey2];
			int result = value2 - value1;
			if (result == 0) {
				value1 = this.sortKeys[o1.intValue()][this.activeKey1];
				value2 = this.sortKeys[o2.intValue()][this.activeKey1];
			}
			return value2 - value1;
		*/	
		}
		
		private int increment(int number) {
			return (number == this.sortKeys.get(0).length - 1) ? 0 : number + 1;
		}
	}

	private class DistributedAttributeRepresentant implements Comparable<DistributedAttributeRepresentant>, Serializable {
		
		private int windowDistance = 0;
		private IntArrayList numNewNonFds = new IntArrayList();
		private IntArrayList numComparisons = new IntArrayList();
		private int attribute;
		private int numberTuples;
		//private List<IntArrayList> clusters;
		//private FDSet negCover;
		//private FDTree posCover;
		//private DistributedSampler sampler;
		//private MemoryGuardian memoryGuardian;
		
		public float getEfficiency() {
			int index = this.numNewNonFds.size() - 1;
	/*		int sumNonFds = 0;
			int sumComparisons = 0;
			while ((index >= 0) && (sumComparisons < this.efficiencyFactor)) { //TODO: If we calculate the efficiency with all comparisons and all results in the log, then we can also aggregate all comparisons and results in two variables without maintaining the entire log
				sumNonFds += this.numNewNonFds.getInt(index);
				sumComparisons += this.numComparisons.getInt(index);
				index--;
			}
			if (sumComparisons == 0)
				return 0;
			return sumNonFds / sumComparisons;
	*/		float sumNewNonFds = this.numNewNonFds.getInt(index);
			float sumComparisons = this.numComparisons.getInt(index);
			if (sumComparisons == 0)
				return 0.0f;
			return sumNewNonFds / sumComparisons;
		}
		
		public DistributedAttributeRepresentant( DistributedSampler sampler, MemoryGuardian memoryGuardian, int attribute, int numberTuples) {
			//this.clusters = new ArrayList<IntArrayList>(clusters);
			this.attribute = attribute;
			this.numberTuples = numberTuples;
			//this.negCover = negCover;
			//this.posCover = posCover;
			//this.sampler = sampler;
			//this.memoryGuardian = memoryGuardian;
		}
		
		@Override
		public int compareTo(DistributedAttributeRepresentant o) {
//			return o.getNumNewNonFds() - this.getNumNewNonFds();		
			return (int)Math.signum(o.getEfficiency() - this.getEfficiency());
		}
		
		/*public void runNext(FDList newNonFds, JavaPairRDD<Integer, Integer[]> compressedRecordsRDD, List<IntArrayList> clusters, JavaSparkContext sc, FDSet negCover) {
			this.windowDistance++;
			int numNewNonFds = 0;
			int numComparisons = 0;
			
			int previousNegCoverSize = newNonFds.size();
			Iterator<IntArrayList> clusterIterator = clusters.iterator();
			while (clusterIterator.hasNext()) {
				IntArrayList cluster = clusterIterator.next();
				
				if (cluster.size() <= this.windowDistance) {
					//clusterIterator.remove();
					continue;
				}
				List<Integer> recordList = Ints.asList(cluster.toIntArray());
				JavaPairRDD<Integer, Boolean> dummyRecordPairRDD = sc.parallelize(recordList).mapToPair(
		    			new PairFunction<Integer, Integer, Boolean>() {
				    		  public Tuple2<Integer, Boolean> call(Integer i) { 
				    			  return new Tuple2<Integer, Boolean>(i, true); 
				    			  }
			    		});
				
				// check if the sort order of records is still preserved
				JavaPairRDD<Integer, Tuple2<Boolean, Integer[]>> requiredRowsPairRDD = dummyRecordPairRDD.join(compressedRecordsRDD);
				// DEBUG
				System.out.println(" - - - JOIN");
				Map<Integer, Tuple2<Boolean, Integer[]>> tmp = requiredRowsPairRDD.collectAsMap();
				for(int i : tmp.keySet()) {
					System.out.print(i+": ");
					for(int j : tmp.get(i)._2)
						System.out.print(j+" ");
					
					System.out.println();
				}
				
				JavaPairRDD<Integer[], Long> requiredRowsRDD = requiredRowsPairRDD.map(new Function<Tuple2<Integer, Tuple2<Boolean, Integer[]>>, Integer[]>() {
					public Integer[] call(Tuple2<Integer, Tuple2<Boolean, Integer[]>> t ) {
						return t._2._2;
					}
				}).zipWithIndex();
				
				JavaPairRDD<Integer, Integer[]> requiredRowsL = requiredRowsRDD.mapToPair(
						new PairFunction<Tuple2<Integer[], Long>, Integer, Integer[]>() {
							public Tuple2<Integer, Integer[]> call(Tuple2<Integer[], Long> t) {
								return new Tuple2<Integer, Integer[]>(t._2.intValue(), t._1);
							}
				});
				
				// DEBUG
				System.out.println(" - - - LEFT SIDE");
				Map<Integer, Integer[]> tmp2 = requiredRowsL.collectAsMap();
				for(int i : tmp2.keySet()) {
					System.out.print(i+": ");
					for(int j : tmp2.get(i))
						System.out.print(j+" ");
					System.out.println();
				}
				
				final Broadcast<Integer> b_windowDistance = sc.broadcast(this.windowDistance);
				final Broadcast<Integer> b_numberRows = sc.broadcast(recordList.size());
				JavaPairRDD<Integer, Integer[]> requiredRowsR = requiredRowsRDD.mapToPair(
						new PairFunction<Tuple2<Integer[], Long>, Integer, Integer[]>() {
							public Tuple2<Integer, Integer[]> call(Tuple2<Integer[], Long> t) {
								return new Tuple2<Integer, Integer[]>((t._2.intValue() + b_windowDistance.value())%b_numberRows.value(), t._1);
							}
				});
				
				// DEBUG
				System.out.println(" - - - RIGHT SIDE");
				Map<Integer, Integer[]> tmp3 = requiredRowsR.collectAsMap();
				for(int i : tmp3.keySet()) {
					System.out.print(i+": ");
					for(int j : tmp3.get(i))
						System.out.print(j+" ");
					System.out.println();
				}
				
				JavaPairRDD<Integer, Tuple2<Integer[], Integer[]>> joinedLR = requiredRowsL.join(requiredRowsR);
				
				// DEBUG
				System.out.println(" - - - RxL JOIN");
				Map<Integer, Tuple2<Integer[], Integer[]>> tmp4 = joinedLR.collectAsMap();
				for(int i : tmp4.keySet()) {
					System.out.print(i+": ");
					for(int j : tmp4.get(i)._1)
						System.out.print(j+" ");
					System.out.print(" | ");
					for(int j : tmp4.get(i)._2)
						System.out.print(j+" ");
					System.out.println();
				}
				
				final Broadcast<Integer> b_numAttributes = sc.broadcast(numAttributes);
				JavaRDD<OpenBitSet> nonFDsRDD = joinedLR.map(
						new Function<Tuple2<Integer, Tuple2<Integer[], Integer[]>>, OpenBitSet>() {
							public OpenBitSet call(Tuple2<Integer, Tuple2<Integer[], Integer[]>> t) {
								Integer[] t1 = t._2._1;
								Integer[] t2 = t._2._2;
								OpenBitSet equalAttrs = new OpenBitSet(b_numAttributes.value());
								//equalAttrs.clear(0, t1.length);
								for (int i = 0; i < t1.length; i++)
									if (t1[i] >= 0 && t2[i] >= 0 && t1[i].equals(t2[i]))
										equalAttrs.set(i);
								
								return equalAttrs;
							}
				});
				
				for(OpenBitSet nonFD : nonFDsRDD.collect()) {
					for(int i = 0; i < posCover.getNumAttributes(); i++)
						if(nonFD.get(i))
							System.out.print(i+" ");
					System.out.println();
					System.out.println("Cardinality, maxDepth: "+ nonFD.cardinality() +" "+ negCover.getMaxDepth());
					if (!negCover.contains(nonFD)) {
						//System.out.println(" Add the above to negCover");
						OpenBitSet equalAttrsCopy = nonFD.clone();
						negCover.add(equalAttrsCopy);
						newNonFds.add(equalAttrsCopy);
					}
				}
				
				// DEBUG
				System.out.println(" Print NegCover: ");
				for(ObjectOpenHashSet<OpenBitSet> hs : negCover.getFdLevels()) {
					for(OpenBitSet b : hs) {
						for(int i = 0; i < posCover.getNumAttributes(); i++)
							if(b.get(i))
								System.out.print(i+" ");
						System.out.println();
					}
				}
				
				numComparisons = numComparisons + new Long(joinedLR.count()).intValue();
				
				for (int recordIndex = 0; recordIndex < (cluster.size() - this.windowDistance); recordIndex++) {
					int recordId = cluster.getInt(recordIndex);
					int partnerRecordId = cluster.getInt(recordIndex + this.windowDistance);
					
					final Broadcast<Integer> b_one = sc.broadcast(recordId);
					final Broadcast<Integer> b_two = sc.broadcast(partnerRecordId);
					Map<Integer, Integer[]> comparePairMap = filterRecords(compressedRecordsRDD, b_one, b_two);
					this.sampler.match(equalAttrs, comparePairMap.get(recordId), comparePairMap.get(partnerRecordId));
					
					if (!this.negCover.contains(equalAttrs)) {
						OpenBitSet equalAttrsCopy = equalAttrs.clone();
						this.negCover.add(equalAttrsCopy);
						newNonFds.add(equalAttrsCopy);
						
						this.memoryGuardian.memoryChanged(1);
						this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
					}
					numComparisons++;
				}
			}
			numNewNonFds = newNonFds.size() - previousNegCoverSize;
			//System.out.println(" numNewNonFds, numComparisons: "+ numNewNonFds+ " " +numComparisons);
			this.numNewNonFds.add(numNewNonFds);
			this.numComparisons.add(numComparisons);
		}*/
		
		public void runNext(FDList newNonFds, JavaPairRDD<Integer, Integer[]> compressedRecordsRDD, List<IntArrayList> clusters, JavaSparkContext sc, FDSet negCover) {
			this.windowDistance++;
			int numNewNonFds = 0;
			int numComparisons = 0;
			int previousNegCoverSize = newNonFds.size();
			
			final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
			final Broadcast<Integer> b_windowDistance = sc.broadcast(this.windowDistance);
			final Broadcast<Integer> b_numAttributes = sc.broadcast(numAttributes);
			
			List<Tuple2<Integer, ArrayList<Tuple2<Integer, Integer[]>>>> pliRowMap = new ArrayList<Tuple2<Integer, ArrayList<Tuple2<Integer, Integer[]>>>>();
			Map<Long, Integer> loadMap = new HashMap<Long, Integer>();
			Map<Long, IntArrayList> pliMap = new HashMap<Long, IntArrayList>();
			for(int i = 0; i < clusters.size(); i++) {
				pliMap.put(new Long(i), clusters.get(i));
				loadMap.put(new Long(i), clusters.get(i).size());
				numComparisons = numComparisons + clusters.get(i).size();
			}
			Object[] sortedLoadMap = sort(loadMap);
			HashMap<Long, Long> packing = pack(sortedLoadMap); // value of the map is the executor ID to which this PLI belongs to.
			Map<Integer, Integer[]> table = compressedRecordsRDD.collectAsMap();
			for(Long pliID : packing.keySet()) {
				Long executorID = packing.get(pliID);
				IntArrayList pli = pliMap.get(pliID);
				ArrayList<Tuple2<Integer, Integer[]>> pliTuples = new ArrayList<Tuple2<Integer, Integer[]>>();
				for(Integer i : pli) {
					pliTuples.add(new Tuple2<Integer, Integer[]>(i, table.get(i)));
				}
				
				pliRowMap.add(new Tuple2<Integer, ArrayList<Tuple2<Integer, Integer[]>>>( executorID.intValue(), pliTuples));
			}
			
			JavaPairRDD<Integer, ArrayList<Tuple2<Integer, Integer[]>>> pliRowMapRDD = sc.parallelizePairs(pliRowMap, numPartitions);
			JavaPairRDD<Integer, Iterable<ArrayList<Tuple2<Integer, Integer[]>>>> groupedRowsRDD = 
										pliRowMapRDD.partitionBy(new Partitioner() {
											@Override
											public int getPartition(Object key) {
												return Integer.parseInt(key.toString());
											}
							
											@Override
											public int numPartitions() {
												return b_numPartitions.value();
											}
										}).groupByKey();
			
			JavaRDD<OpenBitSet> nonFDsRDD = groupedRowsRDD.map(
					new Function<Tuple2<Integer, Iterable<ArrayList<Tuple2<Integer, Integer[]>>>>, Iterable<OpenBitSet>>() {
						public Iterable<OpenBitSet> call(Tuple2<Integer, Iterable<ArrayList<Tuple2<Integer, Integer[]>>>> t) {
							HashSet<OpenBitSet> resSet = new HashSet<OpenBitSet>();
							for(ArrayList<Tuple2<Integer, Integer[]>> l : t._2) {
								for(int i = 0; i < l.size(); i++){
									Integer[] t1 = l.get(i)._2;
									Integer[] t2 = l.get((i+b_windowDistance.value())%l.size())._2;
									OpenBitSet equalAttrs = new OpenBitSet(b_numAttributes.value());
									for (int j = 0; j < t1.length; j++)
										if (t1[j] >= 0 && t2[j] >= 0 && t1[j].equals(t2[j]))
											equalAttrs.set(j);
									resSet.add(equalAttrs);
								}
							}
							return resSet;
						}
			}).flatMap(new FlatMapFunction<Iterable<OpenBitSet>, OpenBitSet>() {
				public Iterator<OpenBitSet> call(Iterable<OpenBitSet> l){
					return l.iterator();
				}
			});
			
			for(OpenBitSet nonFD : nonFDsRDD.collect()) {
				if (!negCover.contains(nonFD)) {
					OpenBitSet equalAttrsCopy = nonFD.clone();
					negCover.add(equalAttrsCopy);
					newNonFds.add(equalAttrsCopy);
				}
			}
			
			numNewNonFds = newNonFds.size() - previousNegCoverSize;
			//System.out.println(" numNewNonFds, numComparisons: "+ numNewNonFds+ " " +numComparisons);
			this.numNewNonFds.add(numNewNonFds);
			this.numComparisons.add(numComparisons);
		}
		
		public Object[] sort(Map<Long, Integer> map) {
			Object[] a = map.entrySet().toArray();
			Arrays.sort(a, new Comparator() {
				public int compare(Object o1, Object o2) {
					return ((Map.Entry<Long, Integer>) o2).getValue().compareTo(((Map.Entry<Long, Integer>) o1).getValue());
				}
			});
			/*
			 * for (Object e : a) { System.out.println(((Map.Entry<Long, Integer>)
			 * e).getKey() + " : " + ((Map.Entry<Long, Integer>) e).getValue()); }
			 */
			return a;
		}
		
		public  HashMap<Long, Long> pack(Object[] a) {
			HashMap<Long, ArrayList<Long>> packing = new HashMap<Long, ArrayList<Long>>();
			HashMap<Long, Long> state = new HashMap<Long, Long>();

			for (int i = 0; i < numPartitions; i++) {
				state.put((new Integer(i)).longValue(), (long) 0);
				packing.put((new Integer(i)).longValue(), new ArrayList<Long>());
			}
			for (Object e : a) {
				Long leastUsedValue = Long.MAX_VALUE;
				Long leastUsedKey = (long) 0;
				// Find the least used key
				for (Long key : state.keySet()) {
					if (state.get(key) < leastUsedValue) {
						leastUsedValue = state.get(key);
						leastUsedKey = key;
					}
				}
				packing.get(leastUsedKey).add(((Map.Entry<Long, Integer>) e).getKey());
				Long newState = state.get(leastUsedKey) + ((Map.Entry<Long, Integer>) e).getValue();
				state.put(leastUsedKey, newState);
			}

			// Maintain inverse of 'packing', i.e. SP id -> packet mapping, for
			// better lookup later.
			HashMap<Long, Long> SPtoPacketMap = new HashMap<Long, Long>();
			for (Long key : packing.keySet()) {
				ArrayList<Long> SPs = packing.get(key);
				for (Long SP : SPs) {
					SPtoPacketMap.put(SP, key);
				}
			}
			return SPtoPacketMap;
		}
	}
	
	private void match(OpenBitSet equalAttrs, int t1, int t2, Map<Integer, Integer[]> compressedRecords) {
		this.match(equalAttrs, compressedRecords.get(t1), compressedRecords.get(t2));
	}
	
	private void match(OpenBitSet equalAttrs, Integer[] t1, Integer[] t2) {
		equalAttrs.clear(0, t1.length);
		for (int i = 0; i < t1.length; i++)
			if (this.valueComparator.isEqual(t1[i], t2[i]))
				equalAttrs.set(i);
	}
}
