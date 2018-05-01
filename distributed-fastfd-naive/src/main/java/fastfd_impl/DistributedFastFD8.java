package fastfd_impl;
/*
 * In this implementation the changes are in how we generate the differenceSet.
 * We do not calculate the maxSets because they seem to be too costly O(n^2).
 * may be repeated difference set calculation is not too expensive.
 * E.g. if [2,3] and [1,2,3,4] both exists as stripped partitions, then we perform
 * differenceSet generation for both.
 * We use Xu's Dis-Dedup+ algorithm for multiple blocking functions.
 * multiblockDiffSet
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
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

public class DistributedFastFD8 implements Serializable
{
	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static Long numberTuples = (long) 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	public static List<Integer> count_dependency;
	public static String datasetFile = null;
	public static boolean debugSysout = false; // for debugging
	public static HashMap<String, String> hashFuncs;
	public static int numReducers = 55;
	public static int sideLen = 10;
	public static Map<String, List<Integer>> BKV2RIDs = new HashMap<String, List<Integer>>();
	public static long genEQClassTime = 0;
	public static long diffJoinTime = 0;
	public static long nonFDtoFDTime = 0;
	
	public static void strippedPartitionGenerator() {
		
		df.cache();
        numberTuples = df.count();
        System.out.println(" # of tuples: " + numberTuples);
        long t1 = System.currentTimeMillis();
        Map<String, Integer> loadMap = new HashMap<String, Integer>();
        JavaPairRDD<String, String> bigHashFuncRDD = sc.parallelizePairs(new ArrayList<Tuple2<String, String>>());
        
        for(int i = 0; i < numberAttributes; i++) {
        	final Broadcast<Integer> b_attribute = sc.broadcast(i);
        	
        	JavaRDD<Row> colRDD = df.select("C"+i).javaRDD();
        	
        	JavaPairRDD<Long, LongList> pairRDD = colRDD.zipWithIndex().mapToPair(
        			new PairFunction<Tuple2<Row,Long>, Long, LongList>() {
		        		public Tuple2<Long, LongList> call(Tuple2<Row,Long> tuple) {
		        			LongList l = new LongArrayList();
		        			l.add(tuple._2);
		        			return new Tuple2<Long, LongList>(tuple._1.getLong(0), l);
		        		}
        	});
        	
        	JavaPairRDD<Long, LongList> strippedPartitionPairRDD = pairRDD.reduceByKey(
        			new Function2<LongList, LongList, LongList>() {
		        		public LongList call(LongList l1, LongList l2) {
		        			LongList l = new LongArrayList(l1);
		        			for(Long e : l2)
		        				l.add(e);
		        			return l;
		        		}
        	}).filter(new Function<Tuple2<Long, LongList>, Boolean>() {
        		public Boolean call(Tuple2<Long, LongList> t) {
        			if(t._2.size() == 0)
        				return false;
        			else
        				return true;
        		}
        	});
        	
        	JavaPairRDD<Long, Integer> loadMapRDD = strippedPartitionPairRDD.mapToPair(
        			new PairFunction<Tuple2<Long, LongList>, Long, Integer>() {
		        		public Tuple2<Long, Integer> call(Tuple2<Long, LongList> t) {
		        			return new Tuple2<Long, Integer>(t._1, t._2.size());
		        		}
        	});
        	
        	Map<Long, Integer> hashFuncLoad = loadMapRDD.collectAsMap();
        	for(Long key : hashFuncLoad.keySet()) {
        		String keyStr = "C"+i + "_"+key;
        		loadMap.put(keyStr, hashFuncLoad.get(key));
        	}
        	
        	JavaPairRDD<String, String> hashFuncRDD = strippedPartitionPairRDD.flatMapToPair(
        			new PairFlatMapFunction<Tuple2<Long, LongList>, String, String>(){
		        		public Iterator<Tuple2<String, String>> call(Tuple2<Long, LongList> t) {
		        			List<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
		        			for(Long i : t._2) {
		        				result.add(new Tuple2<String, String>("C"+b_attribute.value()+"_"+i, "C"+b_attribute.value()+"_"+t._1.intValue()));
		        			}
		        			return result.iterator();
		        		}
        	});
        	bigHashFuncRDD = bigHashFuncRDD.union(hashFuncRDD);
        }
        
        hashFuncs = new HashMap<String, String>(bigHashFuncRDD.collectAsMap());
        long t2 = System.currentTimeMillis();
        genEQClassTime += t2-t1;
        // DEBUG
        /*System.out.println(" :: HASH FUNCTIONS");
        for(String key : hashFuncs.keySet()) {
        	System.out.println(key+" "+hashFuncs.get(key));
        }*/
        
        df.unpersist();
        mapperSetup(loadMap);
        computeDifferenceSetsSpark();
        long t3 = System.currentTimeMillis();
        diffJoinTime += t3 - t2;
	}
    
	public static void mapperSetup(Map<String, Integer> loadMap) {
		Map<String, Float> multiReducerLoad = new HashMap<String, Float>();
		Map<String, Float> singleReducerLoad = new HashMap<String, Float>();
		long W = 0L;
		float Wl = 0f;
		for(String key : loadMap.keySet()) {
			int blockSize = loadMap.get(key);
			W = W + (long) blockSize*(blockSize-1)/2;
		}
		float WoverK = (float) W/numReducers;
		float threshold = (float) ((float) W/(3*numReducers*Math.log(numReducers)));
		System.out.println(" WoverK "+WoverK);
		
		for(String key : loadMap.keySet()) {
			int blockSize = loadMap.get(key);
			float blockLoad = (float) blockSize*(blockSize-1)/2;
			if(blockLoad > WoverK){
				multiReducerLoad.put(key, blockLoad);
				Wl = Wl + blockLoad;
			}
			else if(blockLoad <= WoverK && blockLoad > threshold)
				singleReducerLoad.put(key, blockLoad);
		}
		/*int s = numReducers-1;
		for(String key : multiReducerLoad.keySet()) {
			int k_i = (int) Math.floor((float) multiReducerLoad.get(key)*numReducers/Wl);
			System.out.println(key+" "+k_i+" "+multiReducerLoad.get(key)/Wl);
			List<Integer> RIDS_i = new ArrayList<Integer>();
			for(int j = 0; j < k_i; j++)
				RIDS_i.add(s--);
			BKV2RIDs.put(key, RIDS_i);
		}*/
		for(String key : multiReducerLoad.keySet()) {
			//int k_i = (int) Math.floor((float) multiReducerLoad.get(key)*numReducers/Wl);
			//System.out.println(key+" "+k_i+" "+multiReducerLoad.get(key)/Wl);
			List<Integer> RIDS_i = new ArrayList<Integer>();
			for(int j = 0; j < numReducers; j++)
				RIDS_i.add(j);
			BKV2RIDs.put(key, RIDS_i);
		}
		
		// Sort singleReducerload
		Map<String, Float> sortedSingleReducerLoad = sortByComparator(singleReducerLoad, true);
		
		int RID = 0;
		for(String bkv : sortedSingleReducerLoad.keySet()) {
			List<Integer> rids = new ArrayList<Integer>();
			rids.add(RID%numReducers);
			BKV2RIDs.put(bkv, rids);
			RID++;
		}
		
		// DEBUG
		/*System.out.println(" :: MULTI REDUCER LOAD");
        for(String key : multiReducerLoad.keySet()) {
        	System.out.println(key+" "+multiReducerLoad.get(key));
        }
        System.out.println(" :: SINGLE REDUCER LOAD");
        for(String key : sortedSingleReducerLoad.keySet()) {
        	System.out.println(key+" "+sortedSingleReducerLoad.get(key));
        }
        System.out.println(" :: BKV2RIDs");
        for(String key : BKV2RIDs.keySet()) {
        	System.out.println(key+" "+BKV2RIDs.get(key));
        }*/
	}
	
	public static void computeDifferenceSetsSpark() {
		final Broadcast<HashMap<String, String>> b_hashFuncs = sc.broadcast(hashFuncs);
		final Broadcast<Map<String, List<Integer>>> b_BKV2RIDs = sc.broadcast(BKV2RIDs);
		final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
		final Broadcast<Integer> b_numReducers = sc.broadcast(numReducers);
		JavaRDD<Row> tableRDD = df.javaRDD();
		
		JavaPairRDD<String, Tuple2<String, Row>> rowMap = tableRDD.flatMapToPair(new PairFlatMapFunction<Row, String, Tuple2<String, Row>>() {
    		public Iterator<Tuple2<String, Tuple2<String, Row>>> call(Row r) {
    			List<String> bkvs = new ArrayList<String>();
    			List<Tuple2<String, Tuple2<String, Row>>> result = new ArrayList<Tuple2<String, Tuple2<String, Row>>>();
    			Map<String, String> l_hashFuncs = b_hashFuncs.value();
    			
    			for(int i = 0; i < b_numberAttributes.value(); i++) {
    				String key = "C"+i+"_"+r.getLong(b_numberAttributes.value());
    				if(l_hashFuncs.containsKey(key)){
    					bkvs.add(l_hashFuncs.get(key));
    				}
    			}
    			
    			// if no RID assigned yet, do round robin
    			for(String bkv : bkvs) {
    				if(!b_BKV2RIDs.value().containsKey(bkv)) {
    					int rid = (int)(Math.random()*b_numReducers.value());
    					Tuple2<String, Tuple2<String, Row>> t = 
        						new Tuple2<String, Tuple2<String, Row>>(rid+"_"+bkv, new Tuple2<String, Row>("S", r));
        				result.add(t);
    				} else {
    					List<Integer> RIDs = b_BKV2RIDs.value().get(bkv);
    					int k_i = RIDs.size();
    					//get the largest l, such that l(l+1) / 2 <= k
    					int l = 0;
    					int k0 = (l * (l+1)) / 2;
    					while(k0 <= k_i){
    						l++;
    						k0 = (l * (l+1)) / 2;
    					}
    					l--;
    					
    					int a = (int)(Math.random()*l) + 1;
    					for(int p = 1; p < a; p++) {
    						int ridIndex = getReducerId(p, a, l);
    						int rid = RIDs.get(ridIndex-1);
    						Tuple2<String, Tuple2<String, Row>> t = 
            						new Tuple2<String, Tuple2<String, Row>>((rid)+"_"+bkv, new Tuple2<String, Row>("L", r));
            				result.add(t);
    					}
    					
    					int ridIndex2 = getReducerId(a, a, l);
    					int rid2 = RIDs.get(ridIndex2-1);
    					Tuple2<String, Tuple2<String, Row>> t2 = 
        						new Tuple2<String, Tuple2<String, Row>>((rid2)+"_"+bkv, new Tuple2<String, Row>("S", r));
        				result.add(t2);
        				
        				for(int q = a+1; q <= l; q++) {
        					int ridIndex = getReducerId(a, q, l);
    						int rid = RIDs.get(ridIndex-1);
    						Tuple2<String, Tuple2<String, Row>> t = 
            						new Tuple2<String, Tuple2<String, Row>>((rid)+"_"+bkv, new Tuple2<String, Row>("R", r));
            				result.add(t);
        				}
    				}
    			}
    			
    			return result.iterator();	
    		}
    	});
		
		// DEBUG
		/*for(String key : rowMap.collectAsMap().keySet()) {
			int bkvAttr = Integer.parseInt(key.split("_")[1].split("C")[1]);
			System.out.println("split: "+ key + " "+bkvAttr);
		}*/
		
		JavaPairRDD<String, Iterable<Tuple2<String, Row>>> pairsPartition = rowMap.partitionBy(new Partitioner() {
            @Override
            public int getPartition(Object key) {
            	String complexKey = key.toString();
            	int intKey = Integer.parseInt(complexKey.split("_")[0]);
                return intKey;
            }

            @Override
            public int numPartitions() {
                return b_numReducers.value();
            }
        }).groupByKey();
		
		JavaRDD<String> differenceSetsRDD = pairsPartition.flatMap(
    			new FlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Row>>>, String>() {
    				public Iterator<String> call(Tuple2<String, Iterable<Tuple2<String, Row>>> t) {
    					HashSet<String> differenceSets = new HashSet<String>();
    					//int bkvAttr = Integer.parseInt(t._1.split("_")[1].split("C")[1]);
    					
    					//FDTree negCoverTree = new FDTree(b_numberAttributes.value());
    					List<Row> left = new ArrayList<Row>();
    					List<Row> right = new ArrayList<Row>();
    					List<Row> self = new ArrayList<Row>();
    					for(Tuple2<String, Row> rt: t._2) {
    						if(rt._1.equals("L"))
    							left.add(rt._2);
    						else if(rt._1.equals("R"))
    							right.add(rt._2);
    						else if(rt._1.equals("S"))
    							self.add(rt._2);
    					}
    					if(left.size() != 0 && right.size() != 0) {
    						for(Row r1 : left) {
        						for(Row r2 : right) {
        							/*boolean alreadyCompared = false;
        							Map<String, String> l_hashFuncs = b_hashFuncs.value();
        			    			for(int i = 0; i < bkvAttr; i++) {
        			    				String key1 = "C"+i+"_"+r1.getLong(b_numberAttributes.value());
        			    				String key2 = "C"+i+"_"+r2.getLong(b_numberAttributes.value());
        			    				if(l_hashFuncs.containsKey(key1) && l_hashFuncs.containsKey(key2) 
        			    						&& l_hashFuncs.get(key1).equals(l_hashFuncs.get(key2))){
        			    					alreadyCompared = true;
        			    					break;
        			    				}
        			    			}*/
        			    			//if(!alreadyCompared) {
	        			    			DifferenceSet set = new DifferenceSet();
				    					for(int k = 0; k < b_numberAttributes.value(); k++) {
				    						long val1 = r1.getLong(k);
	        					            long val2 = r2.getLong(k);
				    						if(val1 != val2)
				    							set.add(k);
				    					}
				    					differenceSets.add(set.toString());
        			    			//}
        						}
        					}
    					}
    					else {
    						for(Row r1 : self) {
        						for(Row r2 : self) {
        							/*boolean alreadyCompared = false;
        							Map<String, String> l_hashFuncs = b_hashFuncs.value();
        			    			for(int i = 0; i < bkvAttr; i++) {
        			    				String key1 = "C"+i+"_"+r1.getLong(b_numberAttributes.value());
        			    				String key2 = "C"+i+"_"+r2.getLong(b_numberAttributes.value());
        			    				if(l_hashFuncs.containsKey(key1) && l_hashFuncs.containsKey(key2) 
        			    						&& l_hashFuncs.get(key1).equals(l_hashFuncs.get(key2))){
        			    					alreadyCompared = true;
        			    					break;
        			    				}
        			    			}
        			    			*/
        			    			//if(!alreadyCompared) {
	        							DifferenceSet set = new DifferenceSet();
				    					for(int k = 0; k < b_numberAttributes.value(); k++) {
				    						long val1 = r1.getLong(k);
	        					            long val2 = r2.getLong(k);
				    						if(val1 != val2)
				    							set.add(k);
				    					}
				    					differenceSets.add(set.toString());
        			    			//}
        						}
        					}
    					}
    					return differenceSets.iterator();
    				}
    			});
		
		List<String> differenceSets = new LinkedList<String>();
        DifferenceSet set = new DifferenceSet();
		for(int k = 0; k < b_numberAttributes.value(); k++)
			set.add(k);
		differenceSets.add(set.toString());
		JavaRDD<String> loneRDD = sc.parallelize(differenceSets); // combine this later
		differenceSetsRDD = differenceSetsRDD.union(loneRDD);
		differenceSetsRDD.distinct().repartition(7).saveAsTextFile("hdfs://husky-06:8020/tmp/fastfd/ds");
	}
	
	private static int getReducerId(int i, int j, int sideLen) {
    	int t1 = (2*sideLen - i + 2)*(i-1)/2;
    	int t2 = j - i + 1;
    	return t1 + t2;
    }
	
    private static Map<String, Float> sortByComparator(Map<String, Float> unsortMap, final boolean increasing)
    {

        List<Entry<String, Float>> list = new LinkedList<Entry<String, Float>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Float>>()
        {
            public int compare(Entry<String, Float> o1,
                    Entry<String, Float> o2)
            {
                if (increasing)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
        for (Entry<String, Float> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
    
    public static void findCoversGeneratorSpark() {
    	List<Integer> attributes = new ArrayList<Integer>();
    	for(int i = 0; i < numberAttributes; i++)
    		attributes.add(i);
    	
    	JavaRDD<Integer> attributesRDD = sc.parallelize(attributes);
    	attributesRDD.foreach(new Cover8());
    }
    
	public static void execute() {

		System.out.println("=========== Running DistributedFastFD =========\n");
		System.out.println(" DATASET: " + datasetFile);
        long t1 = System.currentTimeMillis();
        strippedPartitionGenerator();

        long t2 = System.currentTimeMillis();
        findCoversGeneratorSpark();
        long t6 = System.currentTimeMillis();
        nonFDtoFDTime += t6-t2;
        
        System.out.println(" genEQClassTime time(s): " + genEQClassTime/1000);
        System.out.println(" diffJoin time(s): " + diffJoinTime/1000);
        System.out.println(" nonFDtoFD time(s): " + nonFDtoFDTime/1000);
        System.out.println("===== TOTAL time(s): " + (t6-t1)/1000);

    }
	
	public static List<LongList> parsePartitionLine(String line, int numPartitions) {
		List<LongList> partitionedll = new ArrayList<LongList>();
		if(line.length() == 0) // ""
			return partitionedll;
		String str = line.substring(1, line.length()-1); 
		if(str.length() == 0) // []
			return partitionedll;
		
    	for(int j = 0; j < numPartitions; j++){
    		LongList locall = new LongArrayList();
    		partitionedll.add(locall);
    	}
		
		String[] elems = str.split("\\s*,\\s*");
		for(int i = 0; i < elems.length; i++) {
			partitionedll.get(i%numPartitions).add(Long.parseLong(elems[i]));
		}
		return partitionedll;
	}
	
}

class Cover8 implements VoidFunction<Integer> {
	public void call(Integer attribute) { 
		List<DifferenceSet> differenceSets = new LinkedList<DifferenceSet>();
		List<FunctionalDependencyGroup2> result = new LinkedList<FunctionalDependencyGroup2>();
		try{
			Configuration conf = new Configuration();
		    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path("/tmp/fastfd/ds"));
		    for (int i=0;i<status.length;i++){
		    	//System.out.println("blah..."+status[i].getPath());
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	DifferenceSet ds = parseLine(line);
		        	differenceSets.add(ds);
		            line=br.readLine();
		        }
		        br.close();
		    }
		}catch(Exception e){
			e.printStackTrace();
		}
		
        /*for(DifferenceSet as : differenceSets){
        	System.out.println(as.toString_());
        }*/
        
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
        	//fdg.printDependency(b_columnNames.value()); //this.addFdToReceivers(fdg);
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
	
	private static void doRecusiveCrap(int currentAttribute, IntList currentOrdering, List<DifferenceSet> setsNotCovered,
            IntList currentPath, List<DifferenceSet> originalDiffSet, List<FunctionalDependencyGroup2> result, int attribute) {
    	// Basic Case
        // FIXME
        if (!currentOrdering.isEmpty() && /* BUT */setsNotCovered.isEmpty()) {
            //if (debugSysout)
            //    System.out.println("no FDs here");
            return;
        }

        if (setsNotCovered.isEmpty()) {

            List<OpenBitSet> subSets = generateSubSets(currentPath);
            if (noOneCovers(subSets, originalDiffSet)) {
                FunctionalDependencyGroup2 fdg = new FunctionalDependencyGroup2(currentAttribute, currentPath);
                //fdg.printDependency(columnNames);// this.addFdToReceivers(fdg);
                writeFDToFile(fdg.toString(), attribute);
                result.add(fdg);
            } else {
                /*if (debugSysout) {
                    System.out.println("FD not minimal");
                    System.out.println(new FunctionalDependencyGroup2(currentAttribute, currentPath));
                }*/
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
		if(line.length() == 0) // ""
			return ds;
		String str = line.substring(1, line.length()-1); 
		if(str.length() == 0) // []
			return ds;
		String[] attrArray = str.split("\\s*,\\s*");
		for(int i = 0; i < attrArray.length; i++) {
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
			Path path = new Path("/tmp/fastfd/result-"+attribute);
			FSDataOutputStream out;
			if(fs.exists(path))
				out = fs.append(path);
			else
				out = fs.create(path);
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
			br.write(fd+"\n");
			br.close();
			out.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
