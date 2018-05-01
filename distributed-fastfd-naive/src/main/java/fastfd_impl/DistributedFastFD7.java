package fastfd_impl;

/*
 * In this implementation the changes are in how we generate the differenceSet.
 * We do not calculate the maxSets because they seem to be too costly O(n^2).
 * may be repeated difference set calculation is not too expensive.
 * E.g. if [2,3] and [1,2,3,4] both exists as stripped partitions, then we perform
 * differenceSet generation for both.
 * In addition to above, the calculation of differenceSet for each stripped partition is
 * done in parallel.
 * In addition to above, 
 * I avoid reading complete data set into executor memory 
 * while computing diffSet for each stripped partition.
 * Instead, use the stripped partition select the required tuples from data set stored as data frame 
 * and then do difference set computation on that.
 * singleWorkerDiffSet - a smart packing of PLIs to have almost equal sized jobs.
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.lucene.util.OpenBitSet;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import scala.Tuple2;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import fastfd_helper.*;

public class DistributedFastFD7 implements Serializable {
	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static Long numberTuples = (long) 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	public static List<Integer> count_dependency;
	public static String datasetFile = null;
	public static boolean debugSysout = false; // for debugging
	public static int numReducers = 1;
	public static int numPartitions = 1;
	// public static JavaRDD<Row> datasetRDD = null;

	public static void strippedPartitionGenerator() {

		df.cache();
		numberTuples = df.count();
		System.out.println(" # of tuples: " + numberTuples);

		long t1 = System.currentTimeMillis();
		/* BEGIN reading data and save it in to bigRDD */
		JavaRDD<LongList> bigRDD = sc.emptyRDD();
		JavaRDD<Row> datasetRDD = df.javaRDD().cache();
		final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
		for (int i = 0; i < numberAttributes; i++) {

			final Broadcast<Integer> b_attribute = sc.broadcast(i);

			JavaPairRDD<Long, LongList> pairRDD = datasetRDD.mapToPair(new PairFunction<Row, Long, LongList>() {
				public Tuple2<Long, LongList> call(Row r) {
					LongList l = new LongArrayList();
					l.add(r.getLong(r.size() - 1));
					return new Tuple2<Long, LongList>(r.getLong(b_attribute.value()), l);
				}
			});

			JavaPairRDD<Long, LongList> strippedPartitionPairRDD = pairRDD
					.reduceByKey(new Function2<LongList, LongList, LongList>() {
						public LongList call(LongList l1, LongList l2) {
							l1.addAll(l2);
							return l1;
						};
					});

			/* filter all single size partitions */
			strippedPartitionPairRDD = strippedPartitionPairRDD.filter(new Function<Tuple2<Long, LongList>, Boolean>() {
				public Boolean call(Tuple2<Long, LongList> t) {
					int count = 0;
					if (t._2.size() <= 1)
						return false;
					return true;
				}
			});

			JavaRDD<LongList> strippedPartitionsRDD = strippedPartitionPairRDD
					.map(new Function<Tuple2<Long, LongList>, LongList>() {
						public LongList call(Tuple2<Long, LongList> t) {
							// String str = t._2.toString();
							// return str.substring(1, str.length()-1);
							return t._2;
						}
					});

			bigRDD = bigRDD.union(strippedPartitionsRDD);
		}

		JavaPairRDD<Long, LongList> bigRDDZip = bigRDD.zipWithIndex()
				.mapToPair(new PairFunction<Tuple2<LongList, Long>, Long, LongList>() {
					public Tuple2<Long, LongList> call(Tuple2<LongList, Long> t) {
						return new Tuple2<Long, LongList>(t._2, t._1);
					}
				});
		Map<Long, Integer> bigZipCollect = bigRDDZip
				.mapToPair(new PairFunction<Tuple2<Long, LongList>, Long, Integer>() {
					public Tuple2<Long, Integer> call(Tuple2<Long, LongList> t) {
						return new Tuple2<Long, Integer>(t._1, t._2.size());
					}
				}).collectAsMap();

		/*
		 * System.out.println(" Before sorting"); for(Long key :
		 * bigZipCollect.keySet()) { System.out.println(key
		 * +" : "+bigZipCollect.get(key)); }
		 */

		Object[] bigZipCollectSorted = sort(bigZipCollect);
		HashMap<Long, Long> packing = pack(bigZipCollectSorted);
		final Broadcast<HashMap<Long, Long>> b_packing = sc.broadcast(packing);
		/*
		 * System.out.println(" After packing"); for(Long key :
		 * packing.keySet()) { System.out.println(key +" : "+packing.get(key));
		 * }
		 */

		JavaPairRDD<Integer, Iterable<LongList>> bigRDDPacked = bigRDDZip
				.mapToPair(new PairFunction<Tuple2<Long, LongList>, Integer, LongList>() {
					public Tuple2<Integer, LongList> call(Tuple2<Long, LongList> t) {
						Long packet = b_packing.value().get(t._1);
						return new Tuple2<Integer, LongList>(packet.intValue(), t._2);
					}
				}).partitionBy(new Partitioner() {
					@Override
					public int getPartition(Object key) {
						return Integer.parseInt(key.toString());
					}

					@Override
					public int numPartitions() {
						return b_numPartitions.value();
					}
				}).groupByKey();

		// DEBUG
		Map<Integer, Iterable<LongList>> map = bigRDDPacked.collectAsMap();
		for(Integer key : map.keySet()){
			System.out.println(key+":");
			int total = 0;
			for(LongList l : map.get(key)) {
				total += l.size();
				System.out.print(l.size()+",");
			}
			System.out.println("TOTAL: "+total);
		}
		
		long t4 = System.currentTimeMillis();
		System.out.println(" strippedPartitionGeneration time(s): " + (t4 - t1) / 1000);

		computeDifferenceSetsSpark2(bigRDDPacked);
		long t5 = System.currentTimeMillis();
		System.out.println(" differenceSetGenerator time(s): " + (t5 - t4) / 1000);
	}

	/*
	 * In this approach we take HashMaps as input. Each HashMap has a tuple that
	 * needs to be compared (key) and other tuple it needs to be compared
	 * against (as HashSet value).
	 */
	public static void computeDifferenceSetsSpark2(JavaPairRDD<Integer, Iterable<LongList>> bigRDDPacked) {

		final Broadcast<String> b_datasetfile = sc.broadcast(datasetFile);
		final Broadcast<Long> b_numberTuples = sc.broadcast(numberTuples);
		final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);

		List<String> differenceSets = new LinkedList<String>();
		DifferenceSet set = new DifferenceSet();
		for (int k = 0; k < b_numberAttributes.value(); k++)
			set.add(k);
		differenceSets.add(set.toString());
		JavaRDD<String> loneRDD = sc.parallelize(differenceSets); // combine
																	// this
																	// later

		JavaRDD<Set<DifferenceSet>> bigDifferenceSetsRDD = sc.emptyRDD();
		// Read the stripped partitions one at a time
		JavaRDD<Set<DifferenceSet>> differenceSetsRDD = bigRDDPacked
				.map(new Function<Tuple2<Integer, Iterable<LongList>>, Set<DifferenceSet>>() {
					public Set<DifferenceSet> call(Tuple2<Integer, Iterable<LongList>> t) {
						Set<DifferenceSet> differenceSets = new HashSet<DifferenceSet>();

						int[][] table = new int[b_numberTuples.value().intValue()][b_numberAttributes.value()];
						JSONObject obj;
						int currRow = 0;
						try {
							Configuration conf = new Configuration();
							conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
							conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
							FileSystem fs = FileSystem.get(conf);
							FSDataInputStream in = fs.open(new Path(b_datasetfile.value()));
							BufferedReader br = new BufferedReader(new InputStreamReader(in));
							String line = null;
							while ((line = br.readLine()) != null) {
								line = line.substring(0, line.length() - 1);
								obj = (JSONObject) new JSONParser().parse(line);
								for (int i = 0; i < b_numberAttributes.value(); i++) {
									table[currRow][i] = ((Long) obj.get("C" + i)).intValue();
								}
								currRow++;
							}
							br.close();
							in.close();
						} catch (IOException e1) {
							e1.printStackTrace();
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						for (LongList list : t._2) {
							for (int i = 0; i < list.size(); i++) {
								int[] row1 = table[list.get(i).intValue()];
								for (int j = i + 1; j < list.size(); j++) {
									int[] row2 = table[list.get(j).intValue()];
									DifferenceSet set = new DifferenceSet();
									for (int k = 0; k < b_numberAttributes.value(); k++) {
										// System.out.print(row1[k] + "-" +
										// row2[k]+" ");
										if (row1[k] != row2[k])
											set.add(k);
									}
									differenceSets.add(set);
								}
							}
						}

						return differenceSets;
					}
				});
		bigDifferenceSetsRDD = bigDifferenceSetsRDD.union(differenceSetsRDD);

		JavaRDD<String> combinedDiffSetRDD = bigDifferenceSetsRDD
				.flatMap(new FlatMapFunction<Set<DifferenceSet>, String>() {
					public Iterator<String> call(Set<DifferenceSet> s) {
						List<String> result = new LinkedList<String>();
						for (DifferenceSet i : s) {
							result.add(i.toString());
						}
						return result.iterator();
					}
				});

		combinedDiffSetRDD = combinedDiffSetRDD.union(loneRDD);
		combinedDiffSetRDD.distinct().repartition(7).saveAsTextFile("hdfs://husky-06:8020/tmp/fastfd/ds");
	}

	public static void findCoversGeneratorSpark() {
		List<Integer> attributes = new ArrayList<Integer>();
		for (int i = 0; i < numberAttributes; i++)
			attributes.add(i);

		JavaRDD<Integer> attributesRDD = sc.parallelize(attributes);
		attributesRDD.foreach(new Cover7());
	}

	public static void execute() {

		System.out.println("=========== Running DistributedFastFD =========\n");
		System.out.println(" DATASET: " + datasetFile);
		System.out.println(" # Partitions: " + numPartitions);
		long t1 = System.currentTimeMillis();
		strippedPartitionGenerator();

		long t5 = System.currentTimeMillis();
		findCoversGeneratorSpark();
		long t6 = System.currentTimeMillis();
		System.out.println(" findCoversGenerator time(s): " + (t6 - t5) / 1000);

		System.out.println("===== TOTAL time(s): " + (t6 - t1) / 1000);

	}

	public static List<LongList> parsePartitionLine(String line, int numPartitions) {
		List<LongList> partitionedll = new ArrayList<LongList>();
		if (line.length() == 0) // ""
			return partitionedll;
		String str = line.substring(1, line.length() - 1);
		if (str.length() == 0) // []
			return partitionedll;

		for (int j = 0; j < numPartitions; j++) {
			LongList locall = new LongArrayList();
			partitionedll.add(locall);
		}

		String[] elems = str.split("\\s*,\\s*");
		for (int i = 0; i < elems.length; i++) {
			partitionedll.get(i % numPartitions).add(Long.parseLong(elems[i]));
		}
		return partitionedll;
	}

	public static Object[] sort(Map<Long, Integer> map) {
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

	public static HashMap<Long, Long> pack(Object[] a) {
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

class Cover7 implements VoidFunction<Integer> {
	public void call(Integer attribute) {
		List<DifferenceSet> differenceSets = new LinkedList<DifferenceSet>();
		List<FunctionalDependencyGroup2> result = new LinkedList<FunctionalDependencyGroup2>();
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path("/tmp/fastfd/ds"));
			for (int i = 0; i < status.length; i++) {
				// System.out.println("blah..."+status[i].getPath());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
				line = br.readLine();
				while (line != null) {
					DifferenceSet ds = parseLine(line);
					differenceSets.add(ds);
					line = br.readLine();
				}
				br.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*
		 * for(DifferenceSet as : differenceSets){
		 * System.out.println(as.toString_()); }
		 */

		List<DifferenceSet> tempDiffSet = new LinkedList<DifferenceSet>();

		// Try to do it parallely
		// Compute DifferenceSet modulo attribute (line 3 - Fig5 - FastFDs)
		for (DifferenceSet ds : differenceSets) {
			OpenBitSet obs = ds.getAttributes().clone();
			if (!obs.get(attribute)) {
				continue;
			} else {
				obs.flip(attribute);
				tempDiffSet.add(new DifferenceSet(obs));
			}
		}

		// check new DifferenceSet (line 4 + 5 - Fig5 - FastFDs)
		if (tempDiffSet.size() == 0) {
			FunctionalDependencyGroup2 fdg = new FunctionalDependencyGroup2(attribute, new IntArrayList());
			// fdg.printDependency(b_columnNames.value());
			// //this.addFdToReceivers(fdg);
			writeFDToFile(fdg.toString(), attribute);
		} else if (checkNewSet(tempDiffSet)) {
			List<DifferenceSet> copy = new LinkedList<DifferenceSet>();
			copy.addAll(tempDiffSet);
			doRecusiveCrap(attribute, generateInitialOrdering(tempDiffSet), copy, new IntArrayList(), tempDiffSet,
					result, attribute);
		}
	}

	private static boolean checkNewSet(List<DifferenceSet> tempDiffSet) {

		for (DifferenceSet ds : tempDiffSet) {
			if (ds.getAttributes().isEmpty()) {
				return false;
			}
		}

		return true;
	}

	private static IntList generateInitialOrdering(List<DifferenceSet> tempDiffSet) {

		IntList result = new IntArrayList();

		Int2IntMap counting = new Int2IntArrayMap();
		for (DifferenceSet ds : tempDiffSet) {

			int lastIndex = ds.getAttributes().nextSetBit(0);

			while (lastIndex != -1) {
				if (!counting.containsKey(lastIndex)) {
					counting.put(lastIndex, 1);
				} else {
					counting.put(lastIndex, counting.get(lastIndex) + 1);
				}
				lastIndex = ds.getAttributes().nextSetBit(lastIndex + 1);
			}
		}

		// TODO: Comperator und TreeMap --> Tommy
		while (true) {

			if (counting.size() == 0) {
				break;
			}

			int biggestAttribute = -1;
			int numberOfOcc = 0;
			for (int attr : counting.keySet()) {

				if (biggestAttribute < 0) {
					biggestAttribute = attr;
					numberOfOcc = counting.get(attr);
					continue;
				}

				int tempOcc = counting.get(attr);
				if (tempOcc > numberOfOcc) {
					numberOfOcc = tempOcc;
					biggestAttribute = attr;
				} else if (tempOcc == numberOfOcc) {
					if (biggestAttribute > attr) {
						biggestAttribute = attr;
					}
				}
			}

			if (numberOfOcc == 0) {
				break;
			}

			result.add(biggestAttribute);
			counting.remove(biggestAttribute);
		}

		return result;
	}

	private static void doRecusiveCrap(int currentAttribute, IntList currentOrdering,
			List<DifferenceSet> setsNotCovered, IntList currentPath, List<DifferenceSet> originalDiffSet,
			List<FunctionalDependencyGroup2> result, int attribute) {
		// Basic Case
		// FIXME
		if (!currentOrdering.isEmpty() && /* BUT */setsNotCovered.isEmpty()) {
			// if (debugSysout)
			// System.out.println("no FDs here");
			return;
		}

		if (setsNotCovered.isEmpty()) {

			List<OpenBitSet> subSets = generateSubSets(currentPath);
			if (noOneCovers(subSets, originalDiffSet)) {
				FunctionalDependencyGroup2 fdg = new FunctionalDependencyGroup2(currentAttribute, currentPath);
				// fdg.printDependency(columnNames);//
				// this.addFdToReceivers(fdg);
				writeFDToFile(fdg.toString(), attribute);
				result.add(fdg);
			} else {
				/*
				 * if (debugSysout) { System.out.println("FD not minimal");
				 * System.out.println(new
				 * FunctionalDependencyGroup2(currentAttribute, currentPath)); }
				 */
			}

			return;
		}

		// Recusive Case
		for (int i = 0; i < currentOrdering.size(); i++) {

			List<DifferenceSet> next = generateNextNotCovered(currentOrdering.getInt(i), setsNotCovered);
			IntList nextOrdering = generateNextOrdering(next, currentOrdering, currentOrdering.getInt(i));
			IntList currentPathCopy = new IntArrayList(currentPath);
			currentPathCopy.add(currentOrdering.getInt(i));
			doRecusiveCrap(currentAttribute, nextOrdering, next, currentPathCopy, originalDiffSet, result, attribute);
		}
	}

	private static List<OpenBitSet> generateSubSets(IntList currentPath) {

		List<OpenBitSet> result = new LinkedList<OpenBitSet>();

		OpenBitSet obs = new OpenBitSet();
		for (int i : currentPath) {
			obs.set(i);
		}

		for (int i : currentPath) {

			OpenBitSet obs_ = obs.clone();
			obs_.flip(i);
			result.add(obs_);

		}

		return result;
	}

	private static boolean noOneCovers(List<OpenBitSet> subSets, List<DifferenceSet> originalDiffSet) {

		for (OpenBitSet obs : subSets) {

			if (covers(obs, originalDiffSet)) {
				return false;
			}

		}

		return true;
	}

	private static boolean covers(OpenBitSet obs, List<DifferenceSet> originalDiffSet) {

		for (DifferenceSet diff : originalDiffSet) {

			if (OpenBitSet.intersectionCount(obs, diff.getAttributes()) == 0) {
				return false;
			}
		}

		return true;
	}

	private static List<DifferenceSet> generateNextNotCovered(int attribute, List<DifferenceSet> setsNotCovered) {

		List<DifferenceSet> result = new LinkedList<DifferenceSet>();

		for (DifferenceSet ds : setsNotCovered) {

			if (!ds.getAttributes().get(attribute)) {
				result.add(ds);
			}
		}

		return result;
	}

	private static IntList generateNextOrdering(List<DifferenceSet> next, IntList currentOrdering, int attribute) {

		IntList result = new IntArrayList();

		Int2IntMap counting = new Int2IntArrayMap();
		boolean seen = false;
		for (int i = 0; i < currentOrdering.size(); i++) {

			if (!seen) {
				if (currentOrdering.getInt(i) != attribute) {
					continue;
				} else {
					seen = true;
				}
			} else {

				counting.put(currentOrdering.getInt(i), 0);
				for (DifferenceSet ds : next) {

					if (ds.getAttributes().get(currentOrdering.getInt(i))) {
						counting.put(currentOrdering.getInt(i), counting.get(currentOrdering.getInt(i)) + 1);
					}
				}
			}
		}

		// TODO: Comperator und TreeMap --> Tommy
		while (true) {

			if (counting.size() == 0) {
				break;
			}

			int biggestAttribute = -1;
			int numberOfOcc = 0;
			for (int attr : counting.keySet()) {

				if (biggestAttribute < 0) {
					biggestAttribute = attr;
					numberOfOcc = counting.get(attr);
					continue;
				}

				int tempOcc = counting.get(attr);
				if (tempOcc > numberOfOcc) {
					numberOfOcc = tempOcc;
					biggestAttribute = attr;
				} else if (tempOcc == numberOfOcc) {
					if (biggestAttribute > attr) {
						biggestAttribute = attr;
					}
				}
			}

			if (numberOfOcc == 0) {
				break;
			}

			result.add(biggestAttribute);
			counting.remove(biggestAttribute);
		}

		return result;
	}

	public DifferenceSet parseLine(String line) {
		DifferenceSet ds = new DifferenceSet();
		if (line.length() == 0) // ""
			return ds;
		String str = line.substring(1, line.length() - 1);
		if (str.length() == 0) // []
			return ds;
		String[] attrArray = str.split("\\s*,\\s*");
		for (int i = 0; i < attrArray.length; i++) {
			ds.add(Integer.parseInt(attrArray[i]));
		}
		return ds;
	}

	public static void writeFDToFile(String fd, int attribute) {
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/tmp/fastfd/result-" + attribute);
			FSDataOutputStream out;
			if (fs.exists(path))
				out = fs.append(path);
			else
				out = fs.create(path);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));
			br.write(fd + "\n");
			br.close();
			out.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
