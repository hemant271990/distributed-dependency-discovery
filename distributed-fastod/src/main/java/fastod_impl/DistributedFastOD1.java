package fastod_impl;

//import de.metanome.algorithm_integration.AlgorithmExecutionException;
//import de.metanome.algorithm_integration.ColumnCombination;
//import de.metanome.algorithm_integration.ColumnIdentifier;
//import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
//import de.metanome.algorithm_integration.results.FunctionalDependency;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import fastod_helper.*;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;


/**
 * Created by Hemant on 1/31/2018.
 */
public class DistributedFastOD1 {

    private static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level_minus1 = null;
    private static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level0 = null;
    private static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level1 = null;
    private static Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>> prefix_blocks = null;

    private static String tableName;
    public static int numberAttributes;
    public static long numberTuples;
    public static String[] columnNames;
    public static Dataset<Row> df = null;
	public static JavaSparkContext sc;
    private static ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    public static int batch_size = 0;
    private static LongBigArrayBigList tTable;

    private static List<List<String>> rowList;

    public static String DatasetFileName = "";

    public static int MaxRowNumber = 1000000;
    public static int MaxColumnNumber = 1000;
    public static int RunTimeNumber = 6;

    public static String cvsSplitBy = ",";

    public static boolean Prune = true;

    public static boolean FirstTimeRun = true;

    public static boolean InterestingnessPrune = false;

    public static long InterestingnessThreshold = 10000000;

    public static int topk = 100;

    public static boolean BidirectionalTrue = false;

    public static boolean RankDoubleTrue = true;

    public static boolean ReverseRankingTrue = false;

    public static boolean BidirectionalPruneTrue = false;

    public static boolean DoubleAttributes = false;

    public static boolean FindUnionBool = false;

    public static Random Rand = new Random(19999);

    public static boolean reverseRank = true;

    public static int ReverseRankingPercentage = 90; //larger than 100 will always reverse it, negative will be regular ranking

    public static List<String> odList = new ArrayList<String>();

    //OD
    //for each attribute, in order, we have a list of its partition, sorted based on their values in ASC order
    static ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>> TAU_SortedList;
    static ArrayList<ObjectBigArrayBigList<Integer>> attributeValuesList;

    static List<FDODScore1> FDODScoreList;
    static Map<OpenBitSet, Long> XScoreMap = new HashMap<OpenBitSet, Long>();

    int answerCountFD = 1;
    int answerCountOD = 1;
    private static int numberOfOD = 0;
    private static int numberOfFD = 0;
    private static int curr_level = 0;
    public static int sideLen = 10;
    public static int numberExecutors = 55;
    
    public static void execute() {

        FDODScoreList = new ArrayList<FDODScore1>();

        long start1 = System.currentTimeMillis();

        level0 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        level1 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        prefix_blocks = new Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>>();

        // Get information about table from database or csv file
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = loadData();
        setColumnIdentifiers();
        //numberAttributes = columnNames.length;

        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        //Begin Initialize Level 0
        CombinationHelper chLevel0 = new CombinationHelper();
        OpenBitSet rhsCandidatesLevel0 = new OpenBitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
        //chLevel0.setPartition(spLevel0);
        //chLevel0.setUniqueCount(1);
        ObjectArrayList<OpenBitSet> swapCandidatesLevel0 = new ObjectArrayList<OpenBitSet>();//the C_s is empty for L0
        chLevel0.setSwapCandidates(swapCandidatesLevel0);
        chLevel0.setElementCount(spLevel0.getElementCount());
        chLevel0.setError(spLevel0.getError());
        spLevel0 = null;
        level0.put(new OpenBitSet(), chLevel0);
        chLevel0 = null;
        //End Initialize Level 0

        //OD
        TAU_SortedList = new ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>>();
        attributeValuesList = new ArrayList<ObjectBigArrayBigList<Integer>>();

        //Begin Initialize Level 1
        for (int i = 1; i <= numberAttributes; i++) {
            OpenBitSet combinationLevel1 = new OpenBitSet();
            combinationLevel1.set(i);

            CombinationHelper chLevel1 = new CombinationHelper();
            OpenBitSet rhsCandidatesLevel1 = new OpenBitSet();
            rhsCandidatesLevel1.set(1, numberAttributes + 1);
            chLevel1.setRhsCandidates(rhsCandidatesLevel0);

            ObjectArrayList<OpenBitSet> swapCandidatesLevel1 = new ObjectArrayList<OpenBitSet>();//the C_s is empty for L1
            chLevel1.setSwapCandidates(swapCandidatesLevel1);

            reverseRank = false;
            if(Rand.nextInt(100) < ReverseRankingPercentage) {
                reverseRank = true;
            }

            //we also initialize TAU_SortedList with all equivalent classes, even for size 1
            StrippedPartition spLevel1 =
                    new StrippedPartition(partitions.get(i - 1), TAU_SortedList, attributeValuesList, numberTuples);

			//chLevel1.setPartition(spLevel1);
            chLevel1.setElementCount(spLevel1.getElementCount());
            chLevel1.setError(spLevel1.getError());
            
            level1.put(combinationLevel1, chLevel1);
            
            String combination_name = OpenBitSetToBitSetString(combinationLevel1, numberAttributes);
            combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
            
            /* A hacky way to make executor write data to HDFS, because driver on husky-big is not attached to HDFS*/
            Broadcast<String> b_combination_name = sc.broadcast(combination_name);
            List<Integer> attrList = new ArrayList<Integer>();
            attrList.add(i);
            sc.parallelize(attrList).foreach(new VoidFunction<Integer>(){
         	  public void call(Integer i){
         		  try {
		            	   Configuration conf = new Configuration();
						    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
						    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
							FileSystem fs = FileSystem.get(conf);
		                	FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/fastod/1" + "_" + b_combination_name.value()));
		                	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
		            		Gson gson = new Gson();
		            		br.write(gson.toJson(spLevel1));
		            		br.close();
		            		out.close();
		            	} catch (IllegalArgumentException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
         	  }
            });
            
        }
        //End Initialize Level 1

		// DEBUG
        System.out.println("TAU_SortedList");
        for(ObjectBigArrayBigList<LongBigArrayBigList> e : TAU_SortedList)
        	System.out.println(e);
        // DEBUG
        System.out.println("attributeValuesList");
        for(ObjectBigArrayBigList<Integer> e : attributeValuesList)
        	System.out.println(e);

        if(DoubleAttributes){
            DoubleAttributes();
            System.out.println("DoubleAttributes is DONE!");
            return;
        }

        if(FirstTimeRun) {
            System.out.println("# ROW    : " + numberTuples);
            System.out.println("# COLUMN : " + numberAttributes);
            System.out.println("");
        }

        partitions = null;

        long end1 = System.currentTimeMillis();
        //System.out.println("Time Before While Loop : " + (end1 - start1));

        int L = 1;
        while (!level1.isEmpty() && L <= numberAttributes) {
        	curr_level = L;
            //compute dependencies for a level

            System.out.println("LEVEL : " + L + " size : " + level1.size() + " # FD : " + numberOfFD + " # OD : " + numberOfOD);

            computeODs(L);

            // prune the search space
            if(Prune)
                prune(L);

            // compute the combinations for the next level
            generateNextLevel();
            L++;
        }

        if(FirstTimeRun) {
            System.out.println("# FD : " + numberOfFD);
            System.out.println("# OD : " + numberOfOD);
            System.out.println("");
        }

        //sore FDODScoreList

        if(FDODScoreList.size() < topk)
            topk = FDODScoreList.size();

        Collections.sort(FDODScoreList, FDODScore1.FDODScoreComparator());

        /*
        System.out.println("SORTED TOP-K  SORTED TOP-K  SORTED TOP-K  SORTED TOP-K");
        for(int i=0; i<MainClass.topk; i ++){
            FDODScore fdodScore = FDODScoreList.get(i);
            System.out.println((i+1) + "  SCORE: " + fdodScore.score);
            if(fdodScore.functionalDependency != null){
                System.out.print("FD: ");
                System.out.println(fdodScore.functionalDependency);
            }else{
                printOpenBitSetNames("OD: ", fdodScore.X_minus_AB, fdodScore.oneAB);
            }
        }
        */

        return;
    }

    private static ArrayList<ObjectBigArrayBigList<Integer>> DoubleAttributes(){

        ArrayList<ObjectBigArrayBigList<Integer>> newAttributeValuesList = new ArrayList<ObjectBigArrayBigList<Integer>>();

        for(ObjectBigArrayBigList<Integer> attValues : attributeValuesList){

            Set<Integer> uniqueValueSet = new HashSet<Integer>();

            for(int rankedPosition : attValues){
                uniqueValueSet.add(rankedPosition);
            }

            int numberOfUniqeValues = uniqueValueSet.size();

            ObjectBigArrayBigList<Integer> newBigList = new ObjectBigArrayBigList<Integer>();

            for(int rankedPosition : attValues){
                int newRankedPosition = numberOfUniqeValues - rankedPosition;
                newBigList.add(newRankedPosition);
            }

            newAttributeValuesList.add(newBigList);
        }

        try {
            BufferedWriter bw =
                    new BufferedWriter(new FileWriter(DatasetFileName + ".new"));

            ObjectBigArrayBigList<Integer> newBigList1 = newAttributeValuesList.get(0);

            for(int i = 0; i<newBigList1.size64(); i ++){

                for(int j=0; j < newAttributeValuesList.size(); j ++ ){

                    int val = newAttributeValuesList.get(j).get(i);

                    if(j == newAttributeValuesList.size() - 1)
                        bw.write(val+"");
                    else
                        bw.write(val + ",");
                }
                bw.write("\n");
            }
            bw.close();

        }catch(Exception ex){

        }
        return newAttributeValuesList;
    }

    private static void generateNextLevel() {

        //OD
        level_minus1 = level0;

        level0 = level1;
        level1 = null;
        //System.gc();

        //Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> new_level = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();

        buildPrefixBlocks();
        Hashtable<OpenBitSet, Integer> all_combinations = new Hashtable<OpenBitSet, Integer>();
        for (ObjectArrayList<OpenBitSet> prefix_block_list : prefix_blocks.values()) {
            // continue only, if the prefix_block contains at least 2 elements
            if (prefix_block_list.size() < 2) {
                continue;
            }

            ObjectArrayList<OpenBitSet[]> combinations = getListCombinations(prefix_block_list);
            for (OpenBitSet[] c : combinations) {
                OpenBitSet X = (OpenBitSet) c[0].clone();
                X.or(c[1]);

                if (checkSubsets(X)) {
                	all_combinations.put(X, 1);
                }
            }
        }
        
        // SPARK CODE
        final Broadcast<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> b_level0 = sc.broadcast(level0);
		final Broadcast<Long> b_numberTuples = sc.broadcast(numberTuples);
		final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
		final Broadcast<Hashtable<OpenBitSet, Integer>> b_all_combinations = sc.broadcast(all_combinations);
		final Broadcast<Integer> b_cur_level = sc.broadcast(curr_level);
		final Broadcast<Integer> b_sideLen = sc.broadcast(sideLen);
        
		Set<OpenBitSet> keys = level0.keySet();
		List<OpenBitSet> keys_list = new ArrayList(keys);
		/*final Accumulator<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> propertiesAccumulator =
				sc.accumulator(new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>(),
                                new BitSetAndCombinaitonHelperMapAccumulatorParam());*/
		JavaRDD<OpenBitSet> keysRDD = sc.parallelize(keys_list, numberExecutors); //
		
		// DEBUG
		/*for(OpenBitSet obs : keys_list){
			System.out.println(curr_level+" "+OpenBitSetToBitSetString(obs, numberAttributes));
		}*/
		
		// Read data from HDFS and create pairRDD
        JavaPairRDD<OpenBitSet,StrippedPartition> level0RDD = keysRDD.mapToPair(new PairFunction<OpenBitSet, OpenBitSet, StrippedPartition>() {
      	  @Override
      	  public Tuple2<OpenBitSet,StrippedPartition> call(OpenBitSet c) throws Exception {
				String pkg1_combination_name = OpenBitSetToBitSetString(c, b_numberAttributes.value());
				pkg1_combination_name = pkg1_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
				StrippedPartition pkg1_st = null;
				try {
					Configuration conf = new Configuration();
				    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
					FileSystem fs = FileSystem.get(conf);
					FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/fastod/"+ b_cur_level.value()+"_"+pkg1_combination_name));
					BufferedReader br=new BufferedReader(new InputStreamReader(in));
					Gson gson = new Gson();
					pkg1_st = gson.fromJson(br, StrippedPartition.class);
					System.out.println("blah--"+pkg1_st.getError());
					br.close();
					in.close();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
      	    return new Tuple2<OpenBitSet, StrippedPartition>(c, pkg1_st);
      	  }
      	});
        
        // DEBUG
        /*for(StrippedPartition s : level0RDD.collectAsMap().values()){
        	System.out.println(s.getError());
        }*/
        
        // Assign rows to L, s, R executors
        JavaPairRDD<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>> rowMap = level0RDD.flatMapToPair(new PairFlatMapFunction<Tuple2<OpenBitSet,StrippedPartition>, Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>() {
    		public Iterator<Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>> call(Tuple2<OpenBitSet,StrippedPartition> r) {
    			List<Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>> result = new ArrayList<Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>>();
    			int a = (int)(Math.random()*b_sideLen.value()) + 1;
    			for(int p = 1; p < a; p++) {
    				int rid = getReducerId(p, a);
    				Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>> t = 
    						new Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>(rid, new Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>("L", r));
    				result.add(t);
    			}
    			int rid2 = getReducerId(a, a);
    			Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>> t2 = 
						new Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>(rid2, new Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>("S", r));
				result.add(t2);
				for(int q = a+1; q <= b_sideLen.value(); q++) {
					int rid = getReducerId(a, q);
					Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>> t = 
    						new Tuple2<Integer, Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>(rid, new Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>("R", r));
    				result.add(t);
				}
    			return result.iterator();	
    		}
    	});
        
        JavaPairRDD<Integer, Iterable<Tuple2<String, Tuple2<OpenBitSet,StrippedPartition>>>> pairsPartition = rowMap.partitionBy(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return (int) key - 1;
            }

            @Override
            public int numPartitions() {
                return 55;
            }
        }).groupByKey();
        
        JavaRDD<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> htableRDD = pairsPartition.map(new Function<Tuple2< Integer, Iterable<Tuple2< String, Tuple2<OpenBitSet, StrippedPartition> >>>,  Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> () {
      	  public Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> call(Tuple2<Integer, Iterable<Tuple2<String, Tuple2<OpenBitSet, StrippedPartition>>>> t) {
      		  List<Tuple2<OpenBitSet, StrippedPartition>> left = new ArrayList<Tuple2<OpenBitSet, StrippedPartition>>();
				List<Tuple2<OpenBitSet, StrippedPartition>> right = new ArrayList<Tuple2<OpenBitSet, StrippedPartition>>();
				List<Tuple2<OpenBitSet, StrippedPartition>> self = new ArrayList<Tuple2<OpenBitSet, StrippedPartition>>();
				for(Tuple2<String, Tuple2<OpenBitSet, StrippedPartition>> rt: t._2) {
					if(rt._1.equals("L"))
						left.add(rt._2);
					else if(rt._1.equals("R"))
						right.add(rt._2);
					else if(rt._1.equals("S"))
						self.add(rt._2);
				}
				
				Gson gson = new Gson();
				Configuration conf = new Configuration();
			    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
				FileSystem fs = null;
				try {
					fs = FileSystem.get(conf);
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
				Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> htable = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
				
				if(left.size() != 0 && right.size() != 0) {
					for(Tuple2<OpenBitSet, StrippedPartition> e1 : left) {
						for(Tuple2<OpenBitSet, StrippedPartition> e2 : right) {
							OpenBitSet c1_bitset = e1._1;
							OpenBitSet c2_bitset = e2._1;		
							
							OpenBitSet X = (OpenBitSet) c1_bitset.clone();
							X.or(c2_bitset);
							Boolean valid = false;
							if(b_all_combinations.value().containsKey(X))
								valid = true;
							if(!valid)
								continue;
							
							StrippedPartition pkg1_sp = e1._2;
							StrippedPartition pkg2_sp = e2._2; 
							StrippedPartition st = null;
							CombinationHelper ch = new CombinationHelper();
							if (b_level0.value().get(c1_bitset).isValid() && b_level0.value().get(c2_bitset).isValid()) {
							    /*List<Integer> local_tTable = new ArrayList<Integer>(b_numberTuples.value());
							    for (long i = 0; i < b_numberTuples.value(); i++) {
							       local_tTable.add(-1);
							    }*/
							    
								// ====== MULTIPLY ======
								st = multiply(pkg1_sp, pkg2_sp, b_numberTuples.value());
							} else {
							   ch.setInvalid();
							}
							OpenBitSet rhsCandidates = new OpenBitSet();
							ch.setRhsCandidates(rhsCandidates);
							if(st != null) {
								ch.setElementCount(st.getElementCount());
								ch.setError(st.getError());
							}
							
							htable.put(X, ch);
							String combination_name = OpenBitSetToBitSetString(X, b_numberAttributes.value());
							combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
							if(st != null) {
								try {
									FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/fastod/" + (b_cur_level.value()+1)+"_"+combination_name));
									BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
									br.write(gson.toJson(st));
									br.close();
									out.close();
								} catch (IllegalArgumentException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
				else {
					for(Tuple2<OpenBitSet, StrippedPartition> e1 : self) {
						for(Tuple2<OpenBitSet, StrippedPartition> e2 : self) {
							OpenBitSet c1_bitset = e1._1;
							OpenBitSet c2_bitset = e2._1;		
							
							OpenBitSet X = (OpenBitSet) c1_bitset.clone();
							X.or(c2_bitset);
							Boolean valid = false;
							if(b_all_combinations.value().containsKey(X))
								valid = true;
							if(!valid)
								continue;
							
							StrippedPartition pkg1_sp = e1._2;
							StrippedPartition pkg2_sp = e2._2; 
							StrippedPartition st = null;
							CombinationHelper ch = new CombinationHelper();
							if (b_level0.value().get(c1_bitset).isValid() && b_level0.value().get(c2_bitset).isValid()) {
							    /*List<Integer> local_tTable = new ArrayList<Integer>(b_numberTuples.value());
							    for (long i = 0; i < b_numberTuples.value(); i++) {
							       local_tTable.add(-1);
							    }*/
							
							    // ====== MULTIPLY ======
							    st = multiply(pkg1_sp, pkg2_sp, b_numberTuples.value());
							} else {
							   ch.setInvalid();
							}
							OpenBitSet rhsCandidates = new OpenBitSet();
							//ch.setPartition(st);
							ch.setRhsCandidates(rhsCandidates);
							if(st != null) {
								ch.setElementCount(st.getElementCount());
								ch.setError(st.getError());
							}
							htable.put(X, ch);
							String combination_name = OpenBitSetToBitSetString(X, b_numberAttributes.value());
							combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
							if(st != null) {
								try {
									FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/fastod/" + (b_cur_level.value()+1) +"_"+combination_name));
									BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
									br.write(gson.toJson(st));
									br.close();
									out.close();
								} catch (IllegalArgumentException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
				return htable;
				//propertiesAccumulator.add(htable);
      	  }
        });
        
        List<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> htableList = htableRDD.collect();
        Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> new_level = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        for(Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> map : htableList) {
        	for(Entry<OpenBitSet, CombinationHelper> e : map.entrySet()) {
        		new_level.put(e.getKey(), e.getValue());
        	}
        }
        
        level1 = new_level;
    }

    private static int getReducerId(int i, int j) {
      	int t1 = (2*sideLen - i + 2)*(i-1)/2;
      	int t2 = j - i + 1;
      	return t1 + t2;
      }
    
    /**
     * Calculate the product of two stripped partitions and return the result as a new stripped partition.
     *
     * @param pt1: First StrippedPartition
     * @param pt2: Second StrippedPartition
     * @return A new StrippedPartition as the product of the two given StrippedPartitions.
     */
    public static StrippedPartition multiply(StrippedPartition pt1, StrippedPartition pt2, long numTuples) {
        ObjectBigArrayBigList<LongBigArrayBigList> result = new ObjectBigArrayBigList<LongBigArrayBigList>();
        ObjectBigArrayBigList<LongBigArrayBigList> pt1List = pt1.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> pt2List = pt2.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> partition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        long noOfElements = 0;
        
        LongBigArrayBigList tTable_local = new LongBigArrayBigList(numTuples);
        for (long i = 0; i < numTuples; i++) {
        	tTable_local.add(-1);
	    }
        // iterate over first stripped partition and fill tTable.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
            	tTable_local.set(tId, i);
            }
            partition.add(new LongBigArrayBigList());
        }
        // iterate over second stripped partition.
        for (long i = 0; i < pt2List.size64(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable_local.get(t_id) != -1) {
                    partition.get(tTable_local.get(t_id)).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable_local.get(tId) != -1) {
                    if (partition.get(tTable_local.get(tId)).size64() > 1) {
                        LongBigArrayBigList eqClass = partition.get(tTable_local.get(tId));
                        result.add(eqClass);
                        noOfElements += eqClass.size64();
                    }
                    partition.set(tTable_local.get(tId), new LongBigArrayBigList());
                }
            }
        }
        // cleanup tTable to reuse it in the next multiplication.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
            	tTable_local.set(tId, -1);
            }
        }
        return new StrippedPartition(result, noOfElements);
    }

    /**
     * Checks whether all subsets of X (with length of X - 1) are part of the last level.
     * Only if this check return true X is added to the new level.
     *
     * @param X
     * @return
     */
    private static boolean checkSubsets(OpenBitSet X) {
        boolean xIsValid = true;

        // clone of X for usage in the following loop
        OpenBitSet Xclone = (OpenBitSet) X.clone();

        for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
            Xclone.clear(l);
            if (!level0.containsKey(Xclone)) {
                xIsValid = false;
                break;
            }
            Xclone.set(l);
        }

        return xIsValid;
    }

    /**
     * Get all combinations, which can be built out of the elements of a prefix block
     *
     * @param list: List of OpenBitSets, which are in the same prefix block.
     * @return All combinations of the OpenBitSets.
     */
    private static ObjectArrayList<OpenBitSet[]> getListCombinations(ObjectArrayList<OpenBitSet> list) {
        ObjectArrayList<OpenBitSet[]> combinations = new ObjectArrayList<OpenBitSet[]>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                OpenBitSet[] combi = new OpenBitSet[2];
                combi[0] = list.get(a);
                combi[1] = list.get(b);
                combinations.add(combi);
            }
        }
        return combinations;
    }

    /**
     * Build the prefix blocks for a level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes as  the value.
     */
    private static void buildPrefixBlocks() {
        prefix_blocks.clear();
        for (OpenBitSet level_iter : level0.keySet()) {
            OpenBitSet prefix = getPrefix(level_iter);

            if (prefix_blocks.containsKey(prefix)) {
                prefix_blocks.get(prefix).add(level_iter);
            } else {
                ObjectArrayList<OpenBitSet> list = new ObjectArrayList<OpenBitSet>();
                list.add(level_iter);
                prefix_blocks.put(prefix, list);
            }
        }
    }

    /**
     * Get prefix of OpenBitSet by copying it and removing the last Bit.
     *
     * @param bitset
     * @return A new OpenBitSet, where the last set Bit is cleared.
     */
    private static OpenBitSet getPrefix(OpenBitSet bitset) {
        OpenBitSet prefix = (OpenBitSet) bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }

    private static long getLastSetBitIndex(OpenBitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
    }

    /**
     * Prune the current level (level1) by removing all elements with no rhs candidates.
     * All keys are marked as invalid.
     * In case a key is found, minimal dependencies are added to the result receiver.
     *
     * @throws AlgorithmExecutionException if the result receiver cannot handle the functional dependency.
     */
    private static void prune(int L) {

        if(L >= 2) {

            ObjectArrayList<OpenBitSet> elementsToRemove = new ObjectArrayList<OpenBitSet>();
            for (OpenBitSet x : level1.keySet()) {

                if ( level1.get(x).getRhsCandidates().isEmpty() && (level1.get(x).getSwapCandidates().size() == 0) ) {
                    elementsToRemove.add(x);
                    //this continue is useful when we add KEY checking after this if statement
                    continue;
                }

            }

            for (OpenBitSet x : elementsToRemove) {
                level1.remove(x);
            }
        }
    }


    /**
     * Computes the dependencies and ODs for the current level (level1).
     *
     * @throws AlgorithmExecutionException
     */
    private static void computeODs(int L) {

    	// Broadcast these for later use
    	final Broadcast<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> b_level0 = sc.broadcast(level0);
        final Broadcast<ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>>> b_TAU_SortedList = sc.broadcast(TAU_SortedList);
        final Broadcast<ArrayList<ObjectBigArrayBigList<Integer>>> b_attributeValuesList = sc.broadcast(attributeValuesList);
        final Broadcast<Boolean> b_BidirectionalTrue = sc.broadcast(BidirectionalTrue);
        final Broadcast<Boolean> b_BidirectionalPruneTrue = sc.broadcast(BidirectionalPruneTrue);
        final Broadcast<Integer> b_curr_level = sc.broadcast(curr_level);
        final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
        final Broadcast<Long> b_numberTuples = sc.broadcast(numberTuples);
        //System.out.println("Current level inside computeODs: "+curr_level);

        initializeCplus_c_ForLevel(); //Line 2 in Algorithm 3

        //OD
        initializeCplus_s_ForLevel(L);

        // iterate through the combinations of the level
        for (OpenBitSet X : level1.keySet()) {

            //OD remove check for isValid for now
            //if (level1.get(X).isValid()) {

            //*************************** FUNCTIONAL DEPENDENCIES (CANONICAL FORM 1)

            // Build the intersection between X and C_plus(X)
            OpenBitSet C_plus = level1.get(X).getRhsCandidates();
            OpenBitSet intersection = (OpenBitSet) X.clone();
            intersection.intersect(C_plus);

            // clone of X for usage in the following loop
            OpenBitSet Xclone = (OpenBitSet) X.clone();

            // iterate through all elements (A) of the intersection
            for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {
                Xclone.clear(A);

                // check if X\A -> A is valid
                double spXwithoutAError = level0.get(Xclone).getError();
                double spXError = level1.get(X).getError();

                /*if(!BidirectionalPruneTrue){
                    StrippedPartition spXwithoutA_Temp = level0.get(Xclone).getPartition();
                    StrippedPartition spX_Temp = level1.get(X).getPartition();
                    if (spX_Temp.getError() == spXwithoutA_Temp.getError()) {
                        //do nothing
                    }
                }*/

                if (spXError == spXwithoutAError) {

                    //we found one FD here

                    OpenBitSet XwithoutA = (OpenBitSet) Xclone.clone();

                    processFunctionalDependency(XwithoutA, A, null, L);

                    // remove A from C_plus(X)
                    if(Prune)
                        level1.get(X).getRhsCandidates().clear(A);

                    // remove all B in R\X from C_plus(X)
                    if(Prune) {
                        OpenBitSet RwithoutX = new OpenBitSet();

                        // set to R
                        RwithoutX.set(1, numberAttributes + 1);
                        // remove X
                        RwithoutX.andNot(X);

                        for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                            level1.get(X).getRhsCandidates().clear(i);
                        }
                    }

                }
                Xclone.set(A);
            }



            //*************************** ORDER DEPENDENCIES (CANONICAL FORM 2)
            ArrayList<OpenBitSet> removeFromC_s_List = new ArrayList<OpenBitSet>();
            ArrayList<OpenBitSet> swapCandidatesList = new ArrayList<OpenBitSet>();
            final Broadcast<OpenBitSet> b_X = sc.broadcast(X);
            
            for(OpenBitSet bs : level1.get(X).getSwapCandidates()) {
            	swapCandidatesList.add(bs);
            }
            JavaRDD<OpenBitSet> swapCandidatesRDD = sc.parallelize(swapCandidatesList);
            
            //SwapCandidates is C_s_plus
            //for(OpenBitSet oneAB : level1.get(X).getSwapCandidates()){
            JavaRDD<Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>> prunedSwapCandidatesRDD = swapCandidatesRDD.map(new Function<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>(){
            	public Tuple3<OpenBitSet, OpenBitSet, OpenBitSet> call(OpenBitSet oneAB){
            		Tuple3<OpenBitSet, OpenBitSet, OpenBitSet> result = null;
	                //printOpenBitSet("Line 17  and X is : " , X);
	                //printOpenBitSet("Line 17  and AB is : " , oneAB);
	
	                //line 18, Algorithm 3
	                OpenBitSet[] A_B_Separate = getSeparateOpenBitSet_AB(oneAB, b_numberAttributes.value());
	                int[] A_B_Index = getIndexOfOpenBitSet_AB(oneAB, b_numberAttributes.value());
	
	                OpenBitSet A = A_B_Separate[0];
	                OpenBitSet B = A_B_Separate[1];
	
	                int A_index = A_B_Index[0]; //starts from 1
	                int B_index = A_B_Index[1]; //starts from 1
	
	                OpenBitSet X_minus_A = (OpenBitSet) b_X.value().clone();
	                X_minus_A.remove(A);
	                OpenBitSet C_c_X_minus_A = b_level0.value().get(X_minus_A).getRhsCandidates();
	                OpenBitSet C_c_X_minus_A_Clone = C_c_X_minus_A.clone();
	                C_c_X_minus_A_Clone.union(B);
	
	                OpenBitSet X_minus_B = (OpenBitSet) b_X.value().clone();
	                X_minus_B.remove(B);
	                OpenBitSet C_c_X_minus_B = b_level0.value().get(X_minus_B).getRhsCandidates();
	                OpenBitSet C_c_X_minus_B_Clone = C_c_X_minus_B.clone();
	                C_c_X_minus_B_Clone.union(A);
	
	                //this is exactly the if statement in line 18
	                if(  !(C_c_X_minus_B.equals(C_c_X_minus_B_Clone)) ||  !(C_c_X_minus_A.equals(C_c_X_minus_A_Clone))){
	                    //removeFromC_s_List.add(oneAB);
	                	result = new Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>(oneAB, null, null);
	
	                }else{
	
	                    //line 20, if( X\{A,B} : A ~ B)
	
	                    //step 1: find X\{A,B}
	                    OpenBitSet X_minus_AB = (OpenBitSet) b_X.value().clone();
	                    X_minus_AB.remove(A);
	                    X_minus_AB.remove(B);
	
	                    String pkg1_combination_name = OpenBitSetToBitSetString(X_minus_AB, b_numberAttributes.value());
	    				pkg1_combination_name = pkg1_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
	    				StrippedPartition pkg1_st = null;
	    				
	    				if(pkg1_combination_name.equals("")) {
	    					pkg1_st = new StrippedPartition(b_numberTuples.value());
	    				} else {
		    				try {
		    					Configuration conf = new Configuration();
		    				    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		    				    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		    					FileSystem fs = FileSystem.get(conf);
		    					FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/fastod/"+ (b_curr_level.value()-2)+"_"+pkg1_combination_name));
		    					BufferedReader br=new BufferedReader(new InputStreamReader(in));
		    					Gson gson = new Gson();
		    					pkg1_st = gson.fromJson(br, StrippedPartition.class);
		    					br.close();
		    					in.close();
		    				} catch (IllegalArgumentException e) {
		    					e.printStackTrace();
		    				} catch (IOException e) {
		    					e.printStackTrace();
		    				}
	    				}
	    				
	    				
	                    /*ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition_X_minus_AB =
	                            level_minus1.get(X_minus_AB).getPartition().getStrippedPartition();*/
	    				ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition_X_minus_AB = pkg1_st.getStrippedPartition();
	
	                    //printOpenBitSet("X_minus_AB: ", X_minus_AB);
	
	                    //create hash table based on strippedPartition_X_minus_AB
	                    Object2ObjectOpenHashMap<Long, Integer> strippedPartition_X_minus_AB_Hash =
	                            new Object2ObjectOpenHashMap<Long, Integer>();
	                    int counter = 0;
	                    for(LongBigArrayBigList strippedPartitionElement : strippedPartition_X_minus_AB){
	                        for(long element_index : strippedPartitionElement){
	                            strippedPartition_X_minus_AB_Hash.put(element_index, counter);
	                        }
	                        counter ++;
	                    }
	
	                    ObjectBigArrayBigList<LongBigArrayBigList> sorted_TAU_A = b_TAU_SortedList.value().get(A_index - 1);//A_index starts from 1
	
	                    ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>> PI_X_TAU_A_1 =
	                            new ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>>();
	
	                    //PI_X_TAU_A is Table 6 in my Excel file
	                    //the number of items in this list is equal to the number of items in strippedPartition_X_minus_AB
	                    for(LongBigArrayBigList strippedPartitionElement : strippedPartition_X_minus_AB){
	                        PI_X_TAU_A_1.add(new ObjectBigArrayBigList<LongBigArrayBigList>());
	                    }
	
	                    for(LongBigArrayBigList tau_A_element : sorted_TAU_A){
	
	                        Set<Integer> seenIndexSet = new HashSet<Integer>();
	                        for(long l_a : tau_A_element){
	                            //insert in PI_X_TAU_A
	                            if(strippedPartition_X_minus_AB_Hash.containsKey(l_a)) {
	
	                                int index_in_PI_X_TAU_A = strippedPartition_X_minus_AB_Hash.get(l_a);
	                                if (seenIndexSet.contains(index_in_PI_X_TAU_A)) {
	                                    //In this case, this will be added to the last list
	                                } else {
	                                    //In this case, a new list is created
	                                    seenIndexSet.add(index_in_PI_X_TAU_A);
	                                    PI_X_TAU_A_1.get(index_in_PI_X_TAU_A).add(new LongBigArrayBigList());
	                                }
	
	                                long currentSize = PI_X_TAU_A_1.get(index_in_PI_X_TAU_A).size64();
	                                PI_X_TAU_A_1.get(index_in_PI_X_TAU_A).get(currentSize - 1).add(l_a);
	
	                            }
	                        }
	                    }
	
	
	                    //PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD
	                    if(!b_BidirectionalPruneTrue.value()){
	
	                        ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>> PI_X_TAU_A_2 =
	                                new ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>>();
	
	                        for(LongBigArrayBigList strippedPartitionElement : strippedPartition_X_minus_AB){
	                            PI_X_TAU_A_2.add(new ObjectBigArrayBigList<LongBigArrayBigList>());
	                        }
	
	                        for(LongBigArrayBigList tau_A_element : sorted_TAU_A){
	
	                            Set<Integer> seenIndexSet = new HashSet<Integer>();
	                            for(long l_a : tau_A_element){
	                                //insert in PI_X_TAU_A_TEMP
	                                if(strippedPartition_X_minus_AB_Hash.containsKey(l_a)) {
	
	                                    int index_in_PI_X_TAU_A_TEMP = strippedPartition_X_minus_AB_Hash.get(l_a);
	                                    if (seenIndexSet.contains(index_in_PI_X_TAU_A_TEMP)) {
	                                        //In this case, this will be added to the last list
	                                    } else {
	                                        //In this case, a new list is created
	                                        seenIndexSet.add(index_in_PI_X_TAU_A_TEMP);
	                                        PI_X_TAU_A_2.get(index_in_PI_X_TAU_A_TEMP).add(new LongBigArrayBigList());
	                                    }
	
	                                    long currentSize = PI_X_TAU_A_2.get(index_in_PI_X_TAU_A_TEMP).size64();
	                                    PI_X_TAU_A_2.get(index_in_PI_X_TAU_A_TEMP).get(currentSize - 1).add(l_a);
	
	                                }
	                            }
	                        }
	
	
	                    }
	                    //PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD
	
	
	                    //check to see whether a swap or reverse swap happened or not
	
	                    ObjectBigArrayBigList<Integer> bValues = b_attributeValuesList.value().get(B_index - 1);
	
	                    //ASC vs ASC
	                    boolean swapHappen = false;
	                    for(int j=0; j<PI_X_TAU_A_1.size64() && (!swapHappen); j ++){
	
	                        ObjectBigArrayBigList oneListInX = PI_X_TAU_A_1.get(j);
	
	                        for(int i=0; i < oneListInX.size64()-1 && (!swapHappen); i ++){
	                            LongBigArrayBigList List1 = (LongBigArrayBigList)oneListInX.get(i);
	                            LongBigArrayBigList List2 = (LongBigArrayBigList)oneListInX.get(i+1);
	
	                            //check to make sure a swap does not happen between List1 and List2 with respect to A and B
	                            int maxB_inList1 = -1;
	                            for(long index1 : List1){
	                                int value = bValues.get(index1);
	                                if(value > maxB_inList1){
	                                    maxB_inList1 = value;
	                                }
	                            }
	
	                            int minB_inList2 = Integer.MAX_VALUE;
	                            for(long index2 : List2){
	                                int value = bValues.get(index2);
	                                if(value < minB_inList2){
	                                    minB_inList2 = value;
	                                }
	                            }
	
	                            //NO Swap: maxB_inList1 < minB_inList2
	                            //Swap: maxB_inList1 > minB_inList2
	                            if(maxB_inList1 > minB_inList2) {
	                                swapHappen = true;
	                            }
	                        }
	                    }
	
	                    //ASC vs DSC
	                    //BOD
	                    boolean swapReverseHappen = false;
	                    if(b_BidirectionalTrue.value()) {
	                        for (int j = 0; j < PI_X_TAU_A_1.size64() && (!swapReverseHappen); j++) {
	
	                            ObjectBigArrayBigList oneListInX = PI_X_TAU_A_1.get(j);
	
	                            for (int i = 0; i < oneListInX.size64() - 1 && (!swapReverseHappen); i++) {
	                                LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
	                                LongBigArrayBigList List2 = (LongBigArrayBigList) oneListInX.get(i + 1);
	
	                                //check to make sure a reverse swap does not happen between List1 and List2 with respect to A and B
	
	                                int minB_inList1 = Integer.MAX_VALUE;
	                                for (long index1 : List1) {
	                                    int value = bValues.get(index1);
	                                    if (value < minB_inList1) {
	                                        minB_inList1 = value;
	                                    }
	                                }
	
	                                int maxB_inList2 = -1;
	                                for (long index2 : List2) {
	                                    int value = bValues.get(index2);
	                                    if (value > maxB_inList2) {
	                                        maxB_inList2 = value;
	                                    }
	                                }
	
	                                //NO Reverse Swap: maxB_inList2 < minB_inList1
	                                //Reverse Swap: maxB_inList2 > minB_inList1
	                                if (maxB_inList2 > minB_inList1) {
	                                    swapReverseHappen = true;
	                                }
	                            }
	                        }
	                    }
	
	
	                    //PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD
	                    if(!b_BidirectionalPruneTrue.value()){
	
	                        boolean swapHappen_temp = false;
	                        for(int j=0; j<PI_X_TAU_A_1.size64() && (!swapHappen_temp); j ++){
	
	                            ObjectBigArrayBigList oneListInX = PI_X_TAU_A_1.get(j);
	
	                            for(int i=0; i < oneListInX.size64()-1 && (!swapHappen_temp); i ++){
	                                LongBigArrayBigList List1 = (LongBigArrayBigList)oneListInX.get(i);
	                                LongBigArrayBigList List2 = (LongBigArrayBigList)oneListInX.get(i+1);
	
	                                //check to make sure a swap does not happen between List1 and List2 with respect to A and B
	                                int maxB_inList1 = -1;
	                                for(long index1 : List1){
	                                    int value = bValues.get(index1);
	                                    if(value > maxB_inList1){
	                                        maxB_inList1 = value;
	                                    }
	                                }
	
	                                int minB_inList2 = Integer.MAX_VALUE;
	                                for(long index2 : List2){
	                                    int value = bValues.get(index2);
	                                    if(value < minB_inList2){
	                                        minB_inList2 = value;
	                                    }
	                                }
	
	                                //NO Swap: maxB_inList1 < minB_inList2
	                                //Swap: maxB_inList1 > minB_inList2
	                                if(maxB_inList1 > minB_inList2) {
	                                    swapHappen_temp = true;
	                                }
	                            }
	                        }
	
	                        //ASC vs DSC
	                        //BOD
	
	                        boolean swapReverseHappen_temp = false;
	                        if(b_BidirectionalTrue.value()) {
	                            for (int j = 0; j < PI_X_TAU_A_1.size64() && (!swapReverseHappen_temp); j++) {
	
	                                ObjectBigArrayBigList oneListInX = PI_X_TAU_A_1.get(j);
	
	                                for (int i = 0; i < oneListInX.size64() - 1 && (!swapReverseHappen_temp); i++) {
	                                    LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
	                                    LongBigArrayBigList List2 = (LongBigArrayBigList) oneListInX.get(i + 1);
	
	                                    //check to make sure a reverse swap does not happen between List1 and List2 with respect to A and B
	
	                                    int minB_inList1 = Integer.MAX_VALUE;
	                                    for (long index1 : List1) {
	                                        int value = bValues.get(index1);
	                                        if (value < minB_inList1) {
	                                            minB_inList1 = value;
	                                        }
	                                    }
	
	                                    int maxB_inList2 = -1;
	                                    for (long index2 : List2) {
	                                        int value = bValues.get(index2);
	                                        if (value > maxB_inList2) {
	                                            maxB_inList2 = value;
	                                        }
	                                    }
	
	                                    //NO Reverse Swap: maxB_inList2 < minB_inList1
	                                    //Reverse Swap: maxB_inList2 > minB_inList1
	                                    if (maxB_inList2 > minB_inList1) {
	                                        swapReverseHappen_temp = true;
	                                    }
	                                }
	                            }
	                        }
	                    }
	                    //PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD PRUNE BOD
	
	                    boolean ODPass = false;
	                    boolean BidODPass = false;
	                    if(b_BidirectionalTrue.value()){
	                        if(!swapHappen){
	                            ODPass = true;
	                        }
	                        if(!swapReverseHappen){
	                            BidODPass = true;
	                        }
	                    }else{
	                        if(!swapHappen){
	                            ODPass = true;
	                        }
	                    }
	
	                    if(ODPass || BidODPass){
	
	                        /* HEMANT: take care of interestingness later
	                        long score = 0;
	                        if(!XScoreMap.containsKey(X_minus_AB)){
	                            score = calculateInterestingnessScore(strippedPartition_X_minus_AB, X_minus_AB);
	                            XScoreMap.put(X_minus_AB, score);
	                        }else{
	                            score = XScoreMap.get(X_minus_AB);
	                        }
	
	                        FDODScore1 fdodScore = new FDODScore1(score, X_minus_AB, oneAB);
	                        FDODScoreList.add(fdodScore);*/
	                        //calculate interestingness score
	
	                        //numberOfOD ++;
	                        //System.out.println("****** OD A SIM B is Found #" + numberOfOD);
	                        //System.out.println("OD#" + (answerCountOD++) + "#SCORE#" + score + "#L#" + L );
	
	                        /*if(ODPass)
	                            printOpenBitSetNames("REG-OD:", X_minus_AB, oneAB);
	                        if(BidODPass)
	                            printOpenBitSetNames("BID-OD:", X_minus_AB, oneAB);*/
	
	                        //System.out.println(score);
	//                        System.out.println("");
	
	                        //removeFromC_s_List.add(oneAB);
	                        result = new Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>(oneAB, X_minus_AB, oneAB);
	                    }
	                }
	                return result;
	            }
            });

            List<Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>> prunedSwapCandidatesList = prunedSwapCandidatesRDD.collect();
            //remove ABs
            if(Prune) {
                for (Tuple3<OpenBitSet, OpenBitSet, OpenBitSet> triple : prunedSwapCandidatesList) {
                	if(triple == null)
                		continue;
                	OpenBitSet removedAB = triple._1();
                    level1.get(X).getSwapCandidates().remove(removedAB);
                    if(triple._2() != null && triple._3() != null) {
                    	numberOfOD ++;
                    	printOpenBitSetNames("OD:", triple._2(), triple._3());
                    }
                }
            }
            //}
        }

    }

    private static long calculateInterestingnessScore(
            ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition,
            OpenBitSet X){
        long score = 0;

        if(X.isEmpty()){
            score = Long.MAX_VALUE;
        }else {

            int totalNumberOfRowsCountedAlready = 0; //this is used to add stirppted partition rows later
            for (LongBigArrayBigList strippedPartitionElement : strippedPartition) {
                score += (strippedPartitionElement.size64() * strippedPartitionElement.size64());
                totalNumberOfRowsCountedAlready += strippedPartitionElement.size64();
            }
            //add the stripped partitions, since each of them is 1, raising them to the power of two will not change their value
            score += (numberTuples - totalNumberOfRowsCountedAlready);
            ;
        }

        return score;
    }

    private static OpenBitSet[] getSeparateOpenBitSet_AB(OpenBitSet oneAB, int numAttr){

        OpenBitSet A = new OpenBitSet();
        OpenBitSet B = new OpenBitSet();

        boolean foundA = false;

        for(int i=0; i<numAttr+1; i ++){
            if(oneAB.get(i)){
                if(!foundA){
                    foundA = true;
                    A.set(i);
                }else{
                    B.set(i);
                }
            }
        }

        OpenBitSet[] results = new OpenBitSet[2];
        results[0] = A;
        results[1] = B;

        return results;
    }

    private static int[] getIndexOfOpenBitSet_AB(OpenBitSet oneAB, int numAttr){

        int A_index = -1;
        int B_index = -1;

        boolean foundA = false;

        for(int i=0; i<numAttr+1; i ++){
            if(oneAB.get(i)){
                if(!foundA){
                    foundA = true;
                    A_index = i;
                }else{
                    B_index = i;
                }
            }
        }

        int[] results = new int[2];
        results[0] = A_index;
        results[1] = B_index;

        return results;
    }

    /**
     * Adds the FD lhs -> a to the resultReceiver and also prints the dependency.
     *
     * @param lhs: left-hand-side of the functional dependency
     * @param a:   dependent attribute. Possible values: 1 <= a <= maxAttributeNumber.
     */
    private static void processFunctionalDependency(OpenBitSet lhs, int a, StrippedPartition spXwithoutA, int L) {
            addDependencyToResultReceiver(lhs, a, spXwithoutA, L);
    }

    private static void addDependencyToResultReceiver(OpenBitSet X, int a, StrippedPartition spXwithoutA, int L) {

        ColumnIdentifier[] columns = new ColumnIdentifier[(int) X.cardinality()];
        int j = 0;
        for (int i = X.nextSetBit(0); i >= 0; i = X.nextSetBit(i + 1)) {
            columns[j++] = columnIdentifiers.get(i - 1);
        }
        ColumnCombination colCombination = new ColumnCombination(columns);
        FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get((int) a - 1));

        /* HEMANT: Not sure what to do with interestingness score
        long score = 0;
        if(!XScoreMap.containsKey(X)){
            score = calculateInterestingnessScore(spXwithoutA.getStrippedPartition(), X);
            XScoreMap.put(X, score);
        }else{
            score = XScoreMap.get(X);
        }

        FDODScore1 fdodScore = new FDODScore1(score, fdResult);
        FDODScoreList.add(fdodScore);*/

        numberOfFD ++;
//        System.out.println("##### FD Found " + numberOfFD);
        //System.out.println("FD#  " + (answerCountFD++) + "  #SCORE#  " + score + "  #L#  " + L );
        //System.out.println(score + "#" +numberOfFD+ "#L#" + L + "#FD:#" + fdResult);
        //System.out.println("FD:" + fdResult);

        //System.out.println(score);
//        System.out.println("");
    }

    //OD
    private static void initializeCplus_s_ForLevel(int L) {

        //ACS  vs   DSC

        if(L == 2){ //Line 3 in Algorithm 3

            for (OpenBitSet X : level1.keySet()) {
                OpenBitSet Xclone = (OpenBitSet) X.clone();

                ObjectArrayList<OpenBitSet> SofX = new ObjectArrayList<OpenBitSet>();
                SofX.add(Xclone);//at level 2, each element X has one C_s

                CombinationHelper ch = level1.get(X);
                ch.setSwapCandidates(SofX);

//                printOpenBitSet("new C+s size is " + SofX.size() + " , and X is : " , X);
//                for(OpenBitSet openAB : SofX){
//                    printOpenBitSet("pair AB: ", openAB);
//                }
            }

        }else{
            if(L > 2){ //Line 5 in Algorithm 3

                //loop through all members of current level, this loop is missing in the pseudo-code
                for (OpenBitSet X : level1.keySet()) {

                    ObjectArrayList<OpenBitSet> allPotentialSwapCandidates = new ObjectArrayList<OpenBitSet>();

                    //clone of X for usage in the following loop
                    //loop over all X\C (line 6 Algorithm 3)
                    OpenBitSet Xclone1 = (OpenBitSet) X.clone();
                    for (int C = X.nextSetBit(0); C >= 0; C = X.nextSetBit(C + 1)) {
                        Xclone1.clear(C);
                        //now Xclone is X/C
                        ObjectArrayList<OpenBitSet> C_s_withoutC_List = level0.get(Xclone1).getSwapCandidates();
                        for(OpenBitSet oneAB : C_s_withoutC_List){
                            if(!allPotentialSwapCandidates.contains(oneAB)){
                                allPotentialSwapCandidates.add(oneAB);
                            }
                        }
                        Xclone1.set(C);
                    }

                    ObjectArrayList<OpenBitSet> allActualSwapCandidates = new ObjectArrayList<OpenBitSet>();

                    //loop over all potential {A,B}
                    for(OpenBitSet oneAB : allPotentialSwapCandidates){
                        //step 1: form X\{A, B}
                        OpenBitSet X_minus_AB = (OpenBitSet) X.clone();
                        X_minus_AB.remove(oneAB);//This is X\{A,B}

                        //now we have to examine all members of X\{A,B}
                        OpenBitSet Xclone2 = (OpenBitSet) X.clone();

                        boolean doesAllofThemContains_AB = true;

                        //loop over X_minus_AB, but check c_s_plus on X_minus_D
                        for (int D = X_minus_AB.nextSetBit(0); D >= 0; D = X_minus_AB.nextSetBit(D + 1)) {
                            Xclone2.clear(D);
                            //now Xclone2 does not contain D
                            ObjectArrayList<OpenBitSet> C_s_withoutD_List = level0.get(Xclone2).getSwapCandidates();
                            if(!C_s_withoutD_List.contains(oneAB))
                                doesAllofThemContains_AB = false;

                            Xclone2.set(D);
                        }

                        if(doesAllofThemContains_AB) {


                            OpenBitSet X__clone_minusAB = (OpenBitSet) X.clone();
                            X__clone_minusAB.remove(oneAB);//This is X\{A,B}
                            //ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition_X_minus_AB =
                              //      level_minus1.get(X__clone_minusAB).getPartition().getStrippedPartition();

                            long score = 0;
                            if(!XScoreMap.containsKey(X__clone_minusAB)){
                                //score = calculateInterestingnessScore(strippedPartition_X_minus_AB, X__clone_minusAB);
                                XScoreMap.put(X__clone_minusAB, score);
                            }else{
                                score = XScoreMap.get(X__clone_minusAB);
                            }

                            if(InterestingnessPrune){
                                //check to see whether we should add oneAB or not
                                if(score > InterestingnessThreshold){
                                    allActualSwapCandidates.add(oneAB);
                                }

                            }else{
                                allActualSwapCandidates.add(oneAB);
                            }

                        }

                    }

                    CombinationHelper ch = level1.get(X);

//                    printOpenBitSet("new C+s size is " + allActualSwapCandidates.size() + " , and X is : " , X);
//                    for(OpenBitSet openAB : allActualSwapCandidates){
//                        printOpenBitSet("pair AB: ", openAB);
//                    }

                    ch.setSwapCandidates(allActualSwapCandidates);

                }
            }
        }
    }

    /**
     * Initialize Cplus_c (resp. rhsCandidates) for each combination of the level.
     */
    private static void initializeCplus_c_ForLevel() {
        for (OpenBitSet X : level1.keySet()) {

            ObjectArrayList<OpenBitSet> CxwithoutA_list = new ObjectArrayList<OpenBitSet>();

            // clone of X for usage in the following loop
            OpenBitSet Xclone = (OpenBitSet) X.clone();
            for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {
                Xclone.clear(A);
                OpenBitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();

                CxwithoutA_list.add(CxwithoutA);
                Xclone.set(A);
            }

            OpenBitSet CforX = new OpenBitSet();

            if (!CxwithoutA_list.isEmpty()) {
                CforX.set(1, numberAttributes + 1);
                for (OpenBitSet CxwithoutA : CxwithoutA_list) {

                    CforX.and(CxwithoutA);

                }
            }

            CombinationHelper ch = level1.get(X);


            OpenBitSet CforX_prune = new OpenBitSet();


            //printOpenBitSet("X: ", X);
            //printOpenBitSet("Cc+: ", CforX);

            boolean isRemovedFromCPlus = false;

            for(int i=1; i<numberAttributes+1; i ++){
                if(CforX.get(i)){

                    if(X.get(i)) {
                        //we have to check the score of X\i
                        OpenBitSet X__clone = (OpenBitSet) X.clone();
                        X__clone.clear(i);
                        //now X__clone is X\A
                        //printOpenBitSet("X\\A: ", X__clone);

                        if(X__clone.isEmpty()){
                            CforX_prune.set(i);
                        }else{
                            //add to the map to improve performance: XScoreMap.containsKey(CforX_clone)

                            StrippedPartition spXwithoutA = level0.get(X__clone).getPartition();

                            long score = 0;
                            if(!XScoreMap.containsKey(X__clone)){
                            	// HEMANT: TODO figure out what to do with the interestingness score
                                //score = calculateInterestingnessScore(spXwithoutA.getStrippedPartition(), X__clone);
                                XScoreMap.put(X__clone, score);
                            }else{
                                score = XScoreMap.get(X__clone);
                            }
                            if (score > InterestingnessThreshold) {
                                CforX_prune.set(i);
                            }else{
                                isRemovedFromCPlus = true;
                            }
                        }
                    }else{
                        //if it is not in X, it should stay in C_c+
                        CforX_prune.set(i);
                    }

                }

            }

            if(InterestingnessPrune){


                if(isRemovedFromCPlus){
                    for(int i=1; i<numberAttributes+1; i ++) {
                        if (CforX_prune.get(i)) {
                            if(X.get(i)) {
                                //do nothing
                            }else{
                                CforX_prune.clear(i);
                            }
                        }
                    }
                }

                //printOpenBitSet("new C+c, and X is : ", X);
                //printOpenBitSet("C+c prune: ", CforX_prune);

                ch.setRhsCandidates(CforX_prune);
            }else{
                ch.setRhsCandidates(CforX);
            }



        }
    }

    private static void setColumnIdentifiers() {
        columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(columnNames.length);
        for (String column_name : columnNames) {
            columnIdentifiers.add(new ColumnIdentifier(tableName, column_name));
        }
    }

    private static void fillData() {
    	rowList = new ArrayList<List<String>>();
    	long rowCount = 0;
    	List<Row> rows = df.collectAsList();
    	for(Row r : rows) {
    		List<String> row = new ArrayList<String>();
    		for(int i = 0; i < numberAttributes; i++) {
    			row.add(new Long(r.getLong(i)).toString());
    		}
    		rowList.add(row);
    		rowCount++;
    	}
    }
    
    
    /*private static void fillData() {

        try {
            br = new BufferedReader(new FileReader(csvFile));

            line = br.readLine();
            String[] attributes = line.split(cvsSplitBy);

            //columnNamesList = new ArrayList<String>();

            long columnCount = 0;
            for(String attributeName : attributes){
                if(columnCount < MaxColumnNumber) {
                    //columnNamesList.add(attributeName);
                    columnCount ++;
                }
            }

            rowList = new ArrayList<List<String>>();

            long rowCount = 0;

            while ( ((line = br.readLine()) != null) && (rowCount < MaxRowNumber)) {

                String[] tuples = line.split(cvsSplitBy);

                List<String> row = new ArrayList<String>();

                long columnCountForThisRow = 0;
                for(String tupleValue : tuples){
                    if(columnCountForThisRow < MaxColumnNumber) {
                        row.add(tupleValue);
                        columnCountForThisRow++;
                    }
                }
                rowList.add(row);

                rowCount ++;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }*/

    private static ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> loadData() {

        fillData();

        //numberAttributes = columnNamesList.size();//input.numberOfColumns();
        tableName = "";//input.relationName();

        //columnNames = columnNamesList;// input.columnNames();

        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions =
                new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(numberAttributes);
        for (int i = 0; i < numberAttributes; i++) {
            Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<Object, LongBigArrayBigList>();
            partitions.add(partition);
        }
        long tupleId = 0;

        //while (input.hasNext()) {
        for(int rowId=0; rowId<rowList.size(); rowId ++) {
            //List<String> row = input.next();
            List<String> row = rowList.get(rowId);

            for (int i = 0; i < numberAttributes; i++) {
                Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = partitions.get(i);
                String entry = row.get(i);
                //System.out.println(entry);
                if (partition.containsKey(entry)) {
                    partition.get(entry).add(tupleId);
                } else {
                    LongBigArrayBigList newEqClass = new LongBigArrayBigList();
                    newEqClass.add(tupleId);
                    partition.put(entry, newEqClass);
                }
            }

            tupleId++;
        }
        numberTuples = tupleId;
        return partitions;

    }

    private static void printOpenBitSet(String message, OpenBitSet bitSet){
        System.out.print(message + "  ");
        for(int i=1; i<numberAttributes+1; i ++){
            if(bitSet.get(i))
                System.out.print(1 + " ");
            else
                System.out.print(0 + " ");
        }
        System.out.println("");
    }
    
    private static String OpenBitSetToBitSetString(OpenBitSet bitSet, int numAttrs){
    	BitSet bs = new BitSet();
    	bs.clear(0);
        for(int i=1; i<numAttrs+1; i ++){
            if(bitSet.get(i))
                bs.set(i);
            else
                bs.clear(i);
        }
        return bs.toString();
    }

    private static void printOpenBitSetNames(String message, OpenBitSet bitSet, OpenBitSet bitSet2){
        System.out.print("" + message + "  ");

        String odVal = "";
        System.out.print("[");
        odVal += "[";
        for(int i=1; i<numberAttributes+1; i ++){
            if(bitSet.get(i)) {
                System.out.print(columnNames[i - 1] + " ");
                odVal += columnNames[i - 1] + " ";
            }
        }
        System.out.print("] Orders [");
        odVal += "] Orders [";

        for(int i=1; i<numberAttributes+1; i ++){
            if(bitSet2.get(i)) {
                System.out.print(columnNames[i - 1] + " ");
                odVal += columnNames[i - 1] + " ";
            }
        }
        System.out.println("]");
        odVal += "]";

        odList.add(odVal);
    }

}

class FDODScore1{
    public long score;
    public OpenBitSet X_minus_AB;
    public OpenBitSet oneAB;
    public FunctionalDependency functionalDependency;

    public FDODScore1(long score, OpenBitSet X_minus_AB, OpenBitSet oneAB){
        score = score;
        X_minus_AB = X_minus_AB;
        oneAB = oneAB;
        functionalDependency = null;
    }

    public FDODScore1(long score, FunctionalDependency functionalDependency){
        score = score;
        X_minus_AB = null;
        oneAB = null;
        functionalDependency = functionalDependency;
    }

    public static Comparator<FDODScore1> FDODScoreComparator(){

        Comparator comp = new Comparator<FDODScore1>(){
            public int compare(FDODScore1 s1, FDODScore1 s2){
                if(s1.score < s2.score)
                    return 1;
                if(s1.score > s2.score)
                    return -1;
                return 0;
            }
        };
        return comp;
    }
}
