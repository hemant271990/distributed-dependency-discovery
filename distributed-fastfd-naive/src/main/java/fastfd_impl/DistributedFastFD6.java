package fastfd_impl;
/* THIS IS THE NAIVE IMPLEMENTATION
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

public class DistributedFastFD6 implements Serializable
{
	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static Long numberTuples = (long) 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	public static List<Integer> count_dependency;
	public static String datasetFile = null;
	public static boolean debugSysout = false; // for debugging
	public static int numPartitions = 1;
	//public static JavaRDD<Row> datasetRDD = null;
	
	public static void strippedPartitionGenerator() {
		
		df.cache();
        numberTuples = df.count();
        System.out.println(" # of tuples: " + numberTuples);
        
        long t1 = System.currentTimeMillis();
        /* BEGIN reading data and save it in to bigRDD*/
        JavaRDD<LongList> bigRDD = sc.emptyRDD();
        JavaRDD<Row> datasetRDD = df.javaRDD().repartition(7).cache();
        final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
        for(int i = 0; i < numberAttributes; i++) {
        	
        	final Broadcast<Integer> b_attribute = sc.broadcast(i);
        	
        	JavaPairRDD<Long, LongList> pairRDD = datasetRDD.mapToPair(
        			new PairFunction<Row, Long, LongList>() {
		        		public Tuple2<Long, LongList> call(Row r) {
		        			LongList l = new LongArrayList();
		        			l.add(r.getLong(r.size()-1));
		        			return new Tuple2<Long, LongList>(r.getLong(b_attribute.value()), l);
		        		}
        	});
        	
        	JavaPairRDD<Long, LongList> strippedPartitionPairRDD = pairRDD.reduceByKey(
        			new Function2<LongList, LongList, LongList>() {
        				public LongList call(LongList l1, LongList l2) {
        					l1.addAll(l2);
        					return l1;
        				};
        	});
        	
        	/*strippedPartitionPairRDD.foreach(new VoidFunction<Tuple2<Long, ArrayList<Long>>>() {
        		public void call(Tuple2<Long, ArrayList<Long>> t){
        			if(t._2.size() > 1){
        				try {
        					FileSystem fs = FileSystem.get(new Configuration());
        					FSDataOutputStream out = fs.create(new Path("/tmp/fastfd/sp/" + b_attribute.value()+"_"+t._1));
        					BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
        					br.write(t._2.toString());
        					//for(long l : t._2)
        					//    br.write(l+"\n");
        					br.close();
        					out.close();
        				} catch (IllegalArgumentException e) {
        					e.printStackTrace();
        				} catch (IOException e) {
        					e.printStackTrace();
        				}
        			}
        		}
        	});*/
        	/* filter all single size partitions*/
        	strippedPartitionPairRDD = strippedPartitionPairRDD.filter(new Function<Tuple2<Long, LongList>, Boolean> () {
	    		public Boolean call(Tuple2<Long, LongList> t) {
	    			int count = 0;
	    			if(t._2.size() <= 1)
	    				return false;
	    			return true;
	    		}
	    	});
        	
        	JavaRDD<LongList> strippedPartitionsRDD = strippedPartitionPairRDD.map(
        			new Function<Tuple2<Long, LongList>, LongList> () {
        				public LongList call(Tuple2<Long, LongList> t) {
        					//String str = t._2.toString();
        					//return str.substring(1, str.length()-1);
        					return t._2;
        				}
        	});
        	
        	bigRDD = bigRDD.union(strippedPartitionsRDD);
        }
        
        JavaPairRDD<Integer, LongList> randomMapSP = bigRDD.mapToPair(
    			new PairFunction<LongList, Integer, LongList>() {
    				public Tuple2<Integer, LongList> call(LongList l) {
    					return new Tuple2<Integer, LongList>((int)(Math.random()*b_numPartitions.value()), l);
				}
    	});
        
        JavaPairRDD<Integer, Iterable<LongList>> pairsPartition = randomMapSP.groupByKey();
    	JavaPairRDD<Tuple2<Integer, Iterable<LongList>>, Tuple2<Integer, Iterable<LongList>>> joinedPairs = pairsPartition.cartesian(pairsPartition);
    	
    	joinedPairs = joinedPairs.filter(new Function<Tuple2<Tuple2<Integer, Iterable<LongList>>, Tuple2<Integer, Iterable<LongList>>>, Boolean> () {
    		public Boolean call(Tuple2<Tuple2<Integer, Iterable<LongList>>, Tuple2<Integer, Iterable<LongList>>> t) {
    			if(t._1._1 > t._2._1)
    				return false;
    			return true;
    		}
    	});
    	
    	JavaRDD<ArrayList<LongList>> toRemove = joinedPairs.repartition(55).map(
    			new Function<Tuple2<Tuple2<Integer, Iterable<LongList>>, Tuple2<Integer, Iterable<LongList>>>, ArrayList<LongList>>() {
		    		public ArrayList<LongList> call(Tuple2<Tuple2<Integer, Iterable<LongList>>, Tuple2<Integer, Iterable<LongList>>> t) {
		    			ArrayList<LongList> result = new ArrayList<LongList>();
		    			
						Iterable<LongList> list1 = t._1._2;
						Iterable<LongList> list2 = t._2._2;
						for(LongList l1 : list1) {
							for(LongList l2 : list2) {
								if(l1.size() < l2.size() && l2.containsAll(l1))
									result.add(l1);
								else if(l1.size() > l2.size() && l1.containsAll(l2))
									result.add(l2);
							}
						}
						return result;
		    		}
    	});
    	
    	JavaRDD<LongList> flatToRemove = toRemove.flatMap(new FlatMapFunction<ArrayList<LongList>, LongList>() {
        	  public Iterator<LongList> call(ArrayList<LongList> s) {
      		  List<LongList> result = new LinkedList<LongList>();
      		  for(LongList i : s) {
      			  result.add(i);
      		  }
      		  return result.iterator(); 
      		  }
      	}).distinct();
    	
    	JavaRDD<String> bigRDDStr = bigRDD.map(
    			new Function<LongList, String>() {
    				public String call(LongList l) {
    					l.sort(null);
    	    			String str = l.toString();
    	    			return str.substring(1, str.length()-1);
    				}
    			});
    	
    	JavaRDD<String> flatToRemoveStr = flatToRemove.map(
    			new Function<LongList, String>() {
    				public String call(LongList l) {
    					l.sort(null);
    	    			String str = l.toString();
    	    			return str.substring(1, str.length()-1);
    				}
    			});
    	bigRDDStr = bigRDDStr.subtract(flatToRemoveStr);

    	bigRDDStr.zipWithIndex().foreach(new VoidFunction<Tuple2<String,Long>>(){
    		public void call(Tuple2<String,Long> t){
				try {
					Configuration conf = new Configuration();
				    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
					FileSystem fs = FileSystem.get(conf);
					FSDataOutputStream out = fs.create(new Path("/tmp/fastfd/sp/" + t._2));
					BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
					br.write(t._1);
					br.close();
					out.close();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    	});
        //bigRDDStr.saveAsTextFile("/tmp/fastfd/sp");
              
        //df.unpersist(); // do not unpersisit, we need it in the next function.
        //System.out.println(" Stripped partitions count: " + bigRDDStr.count());
        //bigRDD.saveAsTextFile("/tmp/fastfd/sp");
        long t4 = System.currentTimeMillis();
        System.out.println(" strippedPartitionGeneration time(s): " + (t4-t1)/1000);
        
        computeDifferenceSetsSpark2();
        long t5 = System.currentTimeMillis();
        System.out.println(" differenceSetGenerator time(s): " + (t5-t4)/1000);
	}
    
    /*
     * In this approach we take HashMaps as input.
     * Each HashMap has a tuple that needs to be compared (key)
     * and other tuple it needs to be compared against (as HashSet value).
     */
    public static void computeDifferenceSetsSpark2() {
    	
        final Broadcast<String> b_datasetfile = sc.broadcast(datasetFile);
        final Broadcast<Long> b_numberTuples = sc.broadcast(numberTuples);
        final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
        
        final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
        List<String> differenceSets = new LinkedList<String>();
        DifferenceSet set = new DifferenceSet();
		for(int k = 0; k < b_numberAttributes.value(); k++)
			set.add(k);
		differenceSets.add(set.toString());
		JavaRDD<String> loneRDD = sc.parallelize(differenceSets); // combine this later
		
        JavaRDD<Set<DifferenceSet>> bigDifferenceSetsRDD = sc.emptyRDD();
        
        /* convert DataFrame into (index, Row) pair*/
    	JavaPairRDD<Integer, Row> indexRowPair = df.javaRDD().mapToPair(
    			new PairFunction<Row, Integer, Row>() {
		    		  public Tuple2<Integer, Row> call(Row r) { 
		    			  return new Tuple2<Integer, Row>((new Long(r.getLong(r.size()-1))).intValue(), r); 
		    			  }
	    		}).cache();
    	df.unpersist();
        // Read the stripped partitions one at a time
        try{
        	Configuration conf = new Configuration();
		    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path("/tmp/fastfd/sp"));
			System.out.println(" status length: " + status.length);
		    for (int i=0;i<status.length;i++){
		    	if(status[i].getPath().getName().compareTo("_SUCCESS") == 0)
		    		continue;
		    	/* Read stripped partitions from file*/
		    	JavaRDD<String> sp = sc.textFile("hdfs://husky-06:8020/tmp/fastfd/sp/"+status[i].getPath().getName()).flatMap(new FlatMapFunction<String, String>() {
		    		  public Iterator<String> call(String s) { return (Iterator<String>) Arrays.asList(s.split("\\s*,\\s*")).iterator(); }
		    	});

		    	/* Convert them to dummy pairs for join later*/
		    	JavaPairRDD<Integer, Boolean> spIdPair = sp.mapToPair(
		    			new PairFunction<String, Integer, Boolean>() {
				    		  public Tuple2<Integer, Boolean> call(String str) { 
				    			  return new Tuple2<Integer, Boolean>(Integer.parseInt(str), true); 
				    			  }
			    		});
		    	
		    	/* Join with stripped partitons to get the required rows out of  the data*/
		    	JavaPairRDD<Integer, Row> requiredRows = spIdPair.join(indexRowPair).mapToPair(
		    			new PairFunction<Tuple2<Integer, Tuple2<Boolean, Row>>, Integer, Row>() {
		    				public Tuple2<Integer, Row> call(Tuple2<Integer, Tuple2<Boolean, Row>> r) {
		    					return new Tuple2<Integer, Row>((int)(Math.random()*b_numPartitions.value()), r._2._2);
		    				}
		    	});
		    	
		    	JavaPairRDD<Integer, Iterable<Row>> pairsPartition = requiredRows.groupByKey();
		    	JavaPairRDD<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> joinedPairs = pairsPartition.cartesian(pairsPartition);
		    	
		    	joinedPairs = joinedPairs.filter(new Function<Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>>, Boolean> () {
		    		public Boolean call(Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> t) {
		    			if(t._1._1 > t._2._1)
		    				return false;
		    			return true;
		    		}
		    	});
		    	
		    	JavaRDD<Set<DifferenceSet>> differenceSetsRDD = joinedPairs.repartition(56).map(
		    			new Function<Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>>, Set<DifferenceSet>>() {
				    		public Set<DifferenceSet> call(Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> t) {
				    			Set<DifferenceSet> differenceSets = new HashSet<DifferenceSet>();
				    			
								Iterable<Row> list1 = t._1._2;
								Iterable<Row> list2 = t._2._2;
								for(Row row1 : list1) {
									for(Row row2 : list2) {
										DifferenceSet set = new DifferenceSet();
				    					for(int k = 0; k < b_numberAttributes.value(); k++) {
				    						//System.out.print(row1[k] + "-" + row2[k]+" ");
				    						if(row1.getLong(k) != row2.getLong(k))
				    							set.add(k);
				    					}
				    					differenceSets.add(set);
									}
								}
								
								return differenceSets;
				    		}
		    	});
		    	//System.out.println(i+": "+differenceSetsRDD.count());
		    	bigDifferenceSetsRDD = bigDifferenceSetsRDD.union(differenceSetsRDD);
		    }
		}catch(Exception e){
			e.printStackTrace();
		}
        
        //System.out.println("differenceSetsRDD count: " + differenceSetsRDD.count());
        
        JavaRDD<String> combinedDiffSetRDD = bigDifferenceSetsRDD.flatMap(new FlatMapFunction<Set<DifferenceSet>, String>() {
      	  public Iterator<String> call(Set<DifferenceSet> s) {
    		  List<String> result = new LinkedList<String>();
    		  for(DifferenceSet i : s) {
    			  result.add(i.toString());
    		  }
    		  return result.iterator(); 
    		  }
    	});
        
        combinedDiffSetRDD = combinedDiffSetRDD.union(loneRDD);
        //System.out.println("combinedDiffSetRDD count: " + combinedDiffSetRDD.distinct().count());
        /*for(String s: combinedDiffSetRDD.distinct().collect())
        	System.out.println(s);*/
        combinedDiffSetRDD.distinct().repartition(7).saveAsTextFile("hdfs://husky-06:8020/tmp/fastfd/ds");
    }
    
    public static void findCoversGeneratorSpark() {
    	List<Integer> attributes = new ArrayList<Integer>();
    	for(int i = 0; i < numberAttributes; i++)
    		attributes.add(i);
    	
    	JavaRDD<Integer> attributesRDD = sc.parallelize(attributes);
    	attributesRDD.foreach(new Cover6());
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
        System.out.println(" findCoversGenerator time(s): " + (t6-t5)/1000);
        
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

class Cover6 implements VoidFunction<Integer> {
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
