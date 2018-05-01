package fastfd_impl;
/*
 * In this implementation the changes are in how we generate the differenceSet.
 * We do not calculate the maxSets because they seem to be too costly O(n^2).
 * may be repeated difference set calculation is not too expensive.
 * E.g. if [2,3] and [1,2,3,4] both exists as stripped partitions, then we perform
 * differenceSet generation for both.
 * In addition to above, the calculation of differenceSet for each stripped partition is
 * done in parallel.
 * In addition to above, we try to read the dataset into an DF and do a join with 
 * stripped partitions to avoid reading dataset from HDFS.
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

import javax.xml.datatype.DatatypeFactory;

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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

public class DistributedFastFD5 implements Serializable
{
	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static Long numberTuples = (long) 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	public static List<Integer> count_dependency;
	public static String datasetFile = null;
	public static boolean debugSysout = false; // for debugging
	public static SparkSession spark;
	private static Dataset<Row> tableDF;
	public static void strippedPartitionGenerator() {
		
		df.repartition(7);
        numberTuples = df.count();
        df.write().format("parquet").save(datasetFile+".pq");
        Dataset<Row> df_pq = spark.read().load(datasetFile+".pq");
        df_pq.createOrReplaceTempView("dataset_t1");
        df_pq.createOrReplaceTempView("dataset_t2");
        
        String selection1 = ""; // used in the select statement
    	for(int k = 0; k < numberAttributes-1; k++)
        	selection1 += " dataset_t1.C"+k+" as t1_C"+k+",";
    	selection1 += " dataset_t1.C"+(numberAttributes-1)+" as t1_C"+(numberAttributes-1);
    	
    	String selection2 = ""; // used in the select statement
    	for(int k = 0; k < numberAttributes-1; k++)
        	selection2 += " dataset_t2.C"+k+" as t2_C"+k+",";
    	selection2 += " dataset_t2.C"+(numberAttributes-1)+" as t2_C"+(numberAttributes-1);
    	
    	Dataset<Row> joinedDataset = spark.sql(
        		"select dataset_t1.index as t1_index, dataset_t2.index as t2_index, "+selection1+", "+selection2+" from dataset_t1, dataset_t2 where dataset_t1.index < dataset_t2.index");
        joinedDataset.cache();
        joinedDataset.createOrReplaceTempView("joined_dataset");
        
        System.out.println(" # of tuples: " + numberTuples);
        System.out.println(" join count: " + joinedDataset.count());
        
        /* BEGIN reading data and save it in to bigRDD*/
        JavaRDD<String> bigRDD = sc.emptyRDD();
        
        for(int i = 0; i < numberAttributes; i++) {
        	
        	final Broadcast<Integer> b_attribute = sc.broadcast(i);
        	
        	JavaRDD<Row> colRDD = df.select("C"+i).javaRDD();
        	
        	JavaPairRDD<Long, LongList> pairRDD = colRDD.zipWithIndex().repartition(7).mapToPair(
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
        	});
        	
        	JavaRDD<String> strippedPartitionsRDD = strippedPartitionPairRDD.map(
        			new Function<Tuple2<Long, LongList>, String> () {
        				public String call(Tuple2<Long, LongList> t) {
        					String str = t._2.toString();
        					return str.substring(1, str.length()-1);
        				}
        	});
        	
        	bigRDD = bigRDD.union(strippedPartitionsRDD);
        	
        }
              
        df.unpersist();
        bigRDD.saveAsTextFile("hdfs://husky-06:8020/tmp/fastfd/sp");
        /*System.out.println("bigRDD count: "+bigRDD.count());
        for(LongList l : bigRDD.collect()) {
    		System.out.println(l);
    	}*/
        long t4 = System.currentTimeMillis();
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
    	
        final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
        
        List<String> differenceSets = new LinkedList<String>();
        DifferenceSet set = new DifferenceSet();
		for(int k = 0; k < b_numberAttributes.value(); k++)
			set.add(k);
		differenceSets.add(set.toString());
		JavaRDD<String> loneRDD = sc.parallelize(differenceSets); // combine this later
		
        JavaRDD<String> bigDifferenceSetsRDD = sc.emptyRDD();
        // Read the stripped partitions one at a time
        try{
        	Configuration conf = new Configuration();
		    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path("/tmp/fastfd/sp"));
		    for (int i=0;i<status.length;i++){
		    	if(status[i].getPath().getName().compareTo("_SUCCESS") == 0)
		    		continue;
		    	JavaRDD<String> sp = sc.textFile("hdfs://husky-06:8020/tmp/fastfd/sp/"+status[i].getPath().getName()).flatMap(new FlatMapFunction<String, String>() {
		    		  public Iterator<String> call(String s) { return (Iterator<String>) Arrays.asList(s.split("\\s*,\\s*")).iterator(); }
		    	});
		    	
		    	String selection1 = ""; // used in the select statement
		    	for(int k = 0; k < numberAttributes-1; k++)
		        	selection1 += " t1_C"+k+",";
		    	selection1 += " t1_C"+(numberAttributes-1);
		    	
		    	String selection2 = ""; // used in the select statement
		    	for(int k = 0; k < numberAttributes-1; k++)
		        	selection2 += " t2_C"+k+",";
		    	selection2 += " t2_C"+(numberAttributes-1);
		    	
		    	JavaRDD<Row> spAsTable = sp.map(new Function<String, Row>() {
		    		public Row call(String s) {
		    			return RowFactory.create(Long.parseLong(s));
		    		}
		    	});
		    	
		    	List<StructField> fields = new ArrayList<StructField>();
		        fields.add(DataTypes.createStructField("spindex", DataTypes.LongType, true));
		        StructType schema = DataTypes.createStructType(fields);
		        Dataset<Row> spDF = spark.createDataFrame(spAsTable, schema);
		        spDF.createOrReplaceTempView("spDF"+i);
		        
		        Dataset<Row> joinedSP = spark.sql("select t1.spindex as t1_spindex, t2.spindex as t2_spindex from spDF"+i+" t1, spDF"+i+" t2 where t1.spindex < t2.spindex");
		        joinedSP.createOrReplaceTempView("joinedSP"+i);
		        
		        JavaRDD<Row> joined = spark.sql("select "+ selection1 + " , "+ selection2 +" from joined_dataset, joinedSP"+i+" where t1_spindex = t1_index and t2_spindex = t2_index").javaRDD();
		    	
		    	JavaRDD<String> differenceSetsRDD = joined.map(
		    			new Function<Row, String>() {
				    		public String call(Row r) {
				    			//Set<DifferenceSet> differenceSets = new HashSet<DifferenceSet>();
				    			
								DifferenceSet set = new DifferenceSet();
		    					for(int k = 0; k < b_numberAttributes.value(); k++) {
		    						if(r.getLong(k) != r.getLong(k+b_numberAttributes.value()))
		    							set.add(k);
		    					}
		    					//differenceSets.add(set);
								
								return set.toString();
				    		}
		    	});
		    	bigDifferenceSetsRDD = bigDifferenceSetsRDD.union(differenceSetsRDD);
		    }
		}catch(Exception e){
			e.printStackTrace();
		}
        
        bigDifferenceSetsRDD = bigDifferenceSetsRDD.union(loneRDD);
        /*System.out.println("combinedDiffSetRDD count: " + bigDifferenceSetsRDD.distinct().count());
        for(String str : bigDifferenceSetsRDD.distinct().collect())
        	System.out.println(str);*/
        
        bigDifferenceSetsRDD.distinct().repartition(7).saveAsTextFile("hdfs://husky-06:8020/tmp/fastfd/ds");
    }
    
    public static void findCoversGeneratorSpark() {
    	List<Integer> attributes = new ArrayList<Integer>();
    	for(int i = 0; i < numberAttributes; i++)
    		attributes.add(i);
    	
    	JavaRDD<Integer> attributesRDD = sc.parallelize(attributes);
    	attributesRDD.foreach(new Cover5());
    }
    
	public static void execute() {

		System.out.println("=========== Running DistributedFastFD =========\n");
		System.out.println(" DATASET: " + datasetFile);
        long t1 = System.currentTimeMillis();
        strippedPartitionGenerator();
        long t2 = System.currentTimeMillis();
        System.out.println(" strippedPartitionGenerator time(s): " + (t2-t1)/1000);

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

class Cover5 implements VoidFunction<Integer> {
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
