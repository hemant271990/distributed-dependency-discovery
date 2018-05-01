package tane_impl;
/*
 * This is a naive implementation of TANE on Spark,
 * where next level is generated as a cross product of
 * previous level. Triangle join is best in this category.
 */
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.String;
import java.util.*;
import java.util.Map.Entry;

import scala.Tuple2;
import tane_helper.*;
import tane_helper.Package;

public class DistributedTane1 {
	private static Hashtable<BitSet, CombinationHelper> level0 = null;
	private static Hashtable<BitSet, CombinationHelper> level1 = null;
	private static Hashtable<Integer, Set<BitSet>> dependency = null;
	private static Hashtable<BitSet, List<BitSet>> prefix_blocks = null;
	private static List<Integer> tTable;
	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static Integer numberTuples = 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	private static long computefd_prune = 0;
	private static long broadcast_package_rdd = 0;
	private static long foreach_rdd = 0;
	private static long generate_next_level = 0;
	private static long foreach_rdd_inside = 0;
	private static Integer combination_counter = 0;
	private static Integer after_checksubset = 0;
	private static Integer multiply_times = 0;
	public static String datasetFile = null;
	public static int numberExecutors = 55;
	public static int fdCount = 0;
	public static long latticeGenTime = 0;
	public static long genEQClassTime = 0;
	public static long pruneTime = 0;
	public static long computeDependencyTime = 0;
	public static int sideLen = 10;
	public static Integer mode = 4; // 1: Spark join driver heavy
									//	2: Spark join write to HDFS
									//	3: Impala join
									//	4: triangle join write to HDFS
									// 	5: triangle join write to HDFS low space but O(n^2) multiplication
	private static int curr_level = 0;

			 public static void execute(){
			        level0 = new Hashtable<BitSet, CombinationHelper>();
			        level1 = new Hashtable<BitSet, CombinationHelper>();
			        dependency = new Hashtable<Integer, Set<BitSet>>();
			        prefix_blocks = new Hashtable<BitSet, List<BitSet>>();
			        // Get information about table from database or csv file
			
			        Long loadingTime_start = System.nanoTime();
			        List<Hashtable<Integer, List<Integer>>> partitions = loadData();
			        long loadingTime = ((System.nanoTime() - loadingTime_start));
			        my_print("total data loading time: " + 0.001*loadingTime/1000000);
			
			
			        Long startTime = System.nanoTime();
			
			        // Initialize table used for stripped partition product
			        tTable = new ArrayList<Integer>(numberTuples);
			        for (long i = 0; i < numberTuples; i++) {
			               tTable.add(-1);
			        }
			
			        // Initialize Level 0
			        CombinationHelper chLevel0 = new CombinationHelper();
			        BitSet rhsCandidatesLevel0 = new BitSet();
			        rhsCandidatesLevel0.set(1, numberAttributes + 1);
			        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
			        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
			        if(mode == 1)
			        	chLevel0.setPartition(spLevel0);
			        chLevel0.setElementCount(spLevel0.getElementCount());
			        chLevel0.setError(spLevel0.getError());
			        spLevel0 = null;
			        level0.put(new BitSet(), chLevel0);
			        chLevel0 = null;
			
			
			        // Initialize Level 1
			        for (int i = 1; i <= numberAttributes; i++) {
			               BitSet combinationLevel1 = new BitSet();
			               combinationLevel1.set(i);
			
			               CombinationHelper chLevel1 = new CombinationHelper();
			               BitSet rhsCandidatesLevel1 = new BitSet();
			               rhsCandidatesLevel1.set(1, numberAttributes + 1);
			               chLevel1.setRhsCandidates(rhsCandidatesLevel0);
			
			               StrippedPartition spLevel1 = new StrippedPartition(partitions.get(i - 1));
			               chLevel1.setElementCount(spLevel1.getElementCount());
			               chLevel1.setError(spLevel1.getError());
			               if(mode == 1)
			            	   chLevel1.setPartition(spLevel1);
			
			               level1.put(combinationLevel1, chLevel1);
			               
			               // write level 1 stripped partitions directly to disk
			               if( mode == 4 || mode == 5) {
				               String combination_name = combinationLevel1.toString();
				               combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
				               
				               try {
				            	   String datasetFile_nice = datasetFile.replaceAll(".json", "").replaceAll("/user/", "").replaceAll("h2saxena/", "");
				            	   Configuration conf = new Configuration();
								    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
								    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
									FileSystem fs = FileSystem.get(conf);
				                	FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/tane_nocache/"+ curr_level+"_"+combination_name));
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
			               } else if( mode == 2) {
			            	   String combination_name = combinationLevel1.toString();
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
						                	FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/tane_nocache/"+ curr_level+"_"+b_combination_name.value()));
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
			        }
			        partitions = null;
			
			        int l = 1;
			
			        long total_compute_dependency = 0;
			        long total_prune = 0;
			        long total_generate_next_level = 0;
			
			
			        while (!level1.isEmpty() && l < numberAttributes) {
			
			        	curr_level = l;

			        	long t1 = System.currentTimeMillis();
			        	computeDependencies();
			        	long t2 = System.currentTimeMillis();
			        	computeDependencyTime += t2-t1;
			        	
			            prune();
			            long t3 = System.currentTimeMillis();
			            pruneTime += t3-t2;
			            
			            Long generate_next_level_start = System.nanoTime();
			               if(mode == 4)
			            	   generateNextLevel_spark(numberExecutors, l);
			               else if(mode == 5)
			            	   generateNextLevel_lowSpace(numberExecutors, l);
			               else if(mode == 1)
			            	   generateNextLevel_baseline_1(l);
			               else if(mode == 2)
			            	   //generateNextLevel_baseline_2a(l);
			            	   generateNextLevel_triangle(l);
			               
			               generate_next_level += (System.nanoTime() - generate_next_level_start);
			
							System.out.println("\n------------------------------ ");
							/*if( (l-1) > 0) {
								String file_nice = datasetFile.replaceAll(".json", "").replaceAll("/user/", "").replaceAll("h2saxena/", "");
								String path = "/tmp/tane_nocache/"+ (l-1) + "_*";
								System.out.println(path);
						        try {
						            Configuration conf = new Configuration();
						            Path output = new Path(path);
						            FileSystem hdfs;
									hdfs = FileSystem.get(conf);
						            //if (hdfs.exists(output)) {
									//	System.out.println("!!! yes it exists !!!");
						                hdfs.delete(output, true);
						            //}
						        } catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}*/

			               l++;
			        }
			        long endTime = System.nanoTime();
			        long duration = (endTime - startTime);
			
			        System.out.println("latticeGenTime: "+latticeGenTime/1000);
			        System.out.println("genEQClassTime: "+genEQClassTime/1000);
			        System.out.println("computeDependencyTime: "+computeDependencyTime/1000);
			        System.out.println("pruneTime: "+pruneTime/1000);
			        
			        /*my_print("time is " + 0.001* duration/1000000 + "sec");
			        my_print("computefd_prune is " + 0.001* computefd_prune/1000000 + "sec");
			        my_print("generate_next_level is " + 0.001* generate_next_level/1000000 + "sec");
			        my_print("broadcast_package_rdd is " + 0.001* broadcast_package_rdd/1000000 + "sec");
			        my_print("foreach_rdd is " + 0.001* foreach_rdd/1000000 + "sec");
			        my_print("foreach_rdd_inside is " + 0.001* foreach_rdd_inside/1000000 + "sec");
			        my_print("total combination_counter: " + combination_counter);
			        my_print("total after_checksubset: " + after_checksubset);
			        my_print("total multiply_times: " + multiply_times);*/
			        my_print("total fd count: " + fdCount);
			 }

             private static List<Hashtable<Integer, List<Integer>>> loadData(){
                List<Hashtable<Integer, List<Integer>>> partitions =
                       new ArrayList<Hashtable<Integer, List<Integer>>>(numberAttributes);

                for (int i = 0; i < columnNames.length; i++) {
                   Hashtable<Integer, List<Integer>> partition =
                          new Hashtable<Integer, List<Integer>>();
                   partitions.add(partition);
                }

                List<Row> table = df.collectAsList();
                numberTuples = table.size();

                Integer tupleId = 0;

                while (tupleId < numberTuples) {
                   Row row = table.get(tupleId);

                   for (int i = 0; i < numberAttributes; i++) {
                      // my_print(((Long)row.get(i)).intValue());

                      Hashtable<Integer, List<Integer>> partition = partitions.get(i);

                      Integer entry = ((Long)row.get(i)).intValue();
                      if (partition.containsKey(entry)) {
                             partition.get(entry).add(tupleId);
                      } else {
                             List<Integer> newEqClass = new ArrayList<Integer>();
                             newEqClass.add(tupleId);
                             partition.put(entry, newEqClass);
                      }
                   }
                   tupleId++;
                   if(tupleId%100000 == 0) my_print("Copied tuple: " + tupleId);
                }

                return partitions;
             }

             private static void initializeCplusForLevel() {
                 for (BitSet X : level1.keySet()) {

                     List<BitSet> CxwithoutA_list = new ArrayList<BitSet>();

                     // clone of X for usage in the following loop
                     BitSet Xclone = (BitSet) X.clone();
                     for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {
                         Xclone.clear(A);
                         BitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                         CxwithoutA_list.add(CxwithoutA);
                         Xclone.set(A);
                     }

                     BitSet CforX = new BitSet();

                     if (!CxwithoutA_list.isEmpty()) {
                         CforX.set(1, numberAttributes + 1);
                         for (BitSet CxwithoutA : CxwithoutA_list) {
                             CforX.and(CxwithoutA);
                         }
                     }

                     CombinationHelper ch = level1.get(X);
                     ch.setRhsCandidates(CforX);
                 }
             }

             private static void computeDependencies() {
                 initializeCplusForLevel();

                 // iterate through the combinations of the level
                 for (BitSet X : level1.keySet()) {
                     if (level1.get(X).isValid()) {
                         // Build the intersection between X and C_plus(X)
                         BitSet C_plus = level1.get(X).getRhsCandidates();
                         BitSet intersection = (BitSet) X.clone();
                         intersection.and(C_plus);

                         // clone of X for usage in the following loop
                         BitSet Xclone = (BitSet) X.clone();

                         // iterate through all elements (A) of the intersection
                         for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {
                             Xclone.clear(A);

                             // check if X\A -> A is valid
                             double spXwithoutA = level0.get(Xclone).getError();
                             double spX = level1.get(X).getError();
                             
                             if (spX == spXwithoutA) {
                                 // found Dependency
                                 BitSet XwithoutA = (BitSet) Xclone.clone();
                                 //processFunctionalDependency(XwithoutA, A);
                                 print_dependency(XwithoutA, A);
                                 // remove A from C_plus(X)
                                 level1.get(X).getRhsCandidates().clear(A);

                                 // remove all B in R\X from C_plus(X)
                                 BitSet RwithoutX = new BitSet();
                                 // set to R
                                 RwithoutX.set(1, numberAttributes + 1);
                                 // remove X
                                 RwithoutX.andNot(X);

                                 for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                     level1.get(X).getRhsCandidates().clear(i);
                                 }

                             }
                             Xclone.set(A);
                         }
                     }
                 }
             }

			private static void prune() {
		        List<BitSet> elementsToRemove = new ArrayList<BitSet>();
		        for (BitSet x : level1.keySet()) {
		            if (level1.get(x).getRhsCandidates().isEmpty()) {
		                elementsToRemove.add(x);
		                continue;
		            }
		            // Check if x is a key. Thats the case, if the error is 0.
		            // See definition of the error on page 104 of the TANE-99 paper.
		            if (level1.get(x).isValid() && level1.get(x).getError() == 0) {

		                // C+(X)\X
		                BitSet rhsXwithoutX = (BitSet) level1.get(x).getRhsCandidates().clone();
		                rhsXwithoutX.andNot(x);
		                for (int a = rhsXwithoutX.nextSetBit(0); a >= 0; a = rhsXwithoutX.nextSetBit(a + 1)) {
		                    BitSet intersect = new BitSet();
		                    intersect.set(1, numberAttributes + 1);

		                    BitSet xUnionAWithoutB = (BitSet) x.clone();
		                    xUnionAWithoutB.set(a);
		                    for (int b = x.nextSetBit(0); b >= 0; b = x.nextSetBit(b + 1)) {
		                        xUnionAWithoutB.clear(b);
		                        CombinationHelper ch = level1.get(xUnionAWithoutB);
		                        if (ch != null) {
		                            intersect.and(ch.getRhsCandidates());
		                        } else {
		                            intersect = new BitSet();
		                            break;
		                        }
		                        xUnionAWithoutB.set(b);
		                    }

		                    if (intersect.get(a)) {
		                        BitSet lhs = (BitSet) x.clone();
		                        //processFunctionalDependency(lhs, a);
		                        print_dependency(lhs, a);
		                        level1.get(x).getRhsCandidates().clear(a);
		                        level1.get(x).setInvalid();
		                    }
		                }
		            }
		        }
		        for (BitSet x : elementsToRemove) {
		            level1.remove(x);
		        }
		    }

			 // lower bound is 0 inclusive
			private static Integer get_a_random_number(Integer exclusive_upper){
				Random random = new Random();
				random.setSeed(System.nanoTime());
				return random.nextInt(exclusive_upper);
			}

			private static void Xu_mapping(List<Packages> package_partitions,
			                                                     BitSet bitset,
			                                                     Integer anchor,
			                                                     Integer parition_number,
			                                                     Integer executor_number_per_row,
			                                                     Integer prefix_index,
			                                                     CombinationHelper ch){
				Package pkg = new Package(prefix_index, bitset, ch);
				//vertical
				Integer index = 0;
				for (Integer p = 0; p < anchor; p++) {
				       index += (anchor - p);
				       // Package pkg_l = new Package(prefix_index, bitset, ch);
				   package_partitions.get(index).l_list.add(pkg);
				
				   // my_print("index: L" + index + "\t" + package_partitions.get(index).l_list.get(package_partitions.get(index).l_list.size() - 1).bitset.toString());
				       index += (executor_number_per_row - (anchor - p) - p);
				}
				
				{
				       // Package pkg_s = new Package(prefix_index, bitset, ch);
				   package_partitions.get(index).s_list.add(pkg);
				   //my_print("index: S" + index + "\t" + pkg_s.ch.getPartition().getStrippedPartition() + "\n"+ package_partitions.get(index).s_list.get(package_partitions.get(index).s_list.size() - 1).ch.getPartition().getStrippedPartition());
				}
				
				for (Integer q = 0; q < executor_number_per_row - anchor - 1; q++) {
				       index++;
				
				       // Package pkg_r = new Package(prefix_index, bitset, ch);
				   package_partitions.get(index).r_list.add(pkg);
				
				   // my_print("index: R" + index + "\t" + package_partitions.get(index).r_list.get(package_partitions.get(index).r_list.size() - 1).bitset.toString());
				}
			
			}


			private static List<Packages> create_package_bag(Integer executor_number_per_row){
			        //according to Trapezoid formula
			        Integer parition_number = (executor_number_per_row * (1 + executor_number_per_row))/2;
			
			        List<Packages> package_partitions = new ArrayList<Packages>(parition_number);
			
			        for (int i = 0; i < parition_number; i++) {
			               package_partitions.add(new Packages());
			        }
			
			        buildPrefixBlocks();
			        Integer prefix_index = 0;
			        for (BitSet prefix: prefix_blocks.keySet()) {
			               if (prefix_blocks.get(prefix).size() <2) {
			                      continue;
			               }
			               // my_print("prefix is " + prefix);
			               for (BitSet bitset : prefix_blocks.get(prefix)) {
			                      // my_print("bitset is " + bitset + "\t" + level0.get(bitset).getPartition().getStrippedPartition());
			                      Integer anchor = get_a_random_number(executor_number_per_row);
			                      Xu_mapping(package_partitions, bitset, anchor, parition_number,
			                             executor_number_per_row, prefix_index, level0.get(bitset));
			               }
			               prefix_index++;
			        }
			
			        return package_partitions;
			 }



              private static void generateNextLevel_spark(Integer executor_number_per_row, Integer cur_level) {
                     level0 = level1;
                     level1 = null;
                     Long broadcast_package_rdd_start = System.nanoTime();

                     //package_partitions size is the same as the number of executors
                     List<Packages> package_partitions = create_package_bag(executor_number_per_row);

                      
                     final Broadcast<Integer> b_numberTuples = sc.broadcast(numberTuples);
                     final Broadcast<Integer> b_cur_level = sc.broadcast(cur_level);
                     
                     HashSet<BitSet> lo_keyset = new HashSet<BitSet>();
                     lo_keyset.addAll(level0.keySet());
                     final Broadcast<HashSet<BitSet>> b_level0_keyset = sc.broadcast(lo_keyset);

                     Integer parition_number = (executor_number_per_row * (1 + executor_number_per_row))/2;

                     Long rdd = System.nanoTime();

                     final Accumulator<Hashtable<BitSet, CombinationHelper>> propertiesAccumulator =
                                   sc.accumulator(new Hashtable<BitSet, CombinationHelper>(),
                                                           new BitSetAndCombinaitonHelperMapAccumulatorParam());
                     
                     JavaRDD<Packages> partitionRDD = sc.parallelize(package_partitions, parition_number).cache();
                     my_print("Create RDD time" + 0.001* (System.nanoTime() - broadcast_package_rdd_start)/1000000 + "sec");

                     broadcast_package_rdd += (System.nanoTime() - broadcast_package_rdd_start);

                     Long foreach_rdd_start = System.nanoTime();
                     
                     partitionRDD.foreach(new VoidFunction<Packages>(){
                            // @Override
                            public void call(Packages pkgs) {
                            	
								List<Package> l_list = pkgs.l_list;
								List<Package> s_list = pkgs.s_list;
								List<Package> r_list = pkgs.r_list;
								
								FileSystem fs = null;
								Gson gson = new Gson();
								try {
									Configuration conf = new Configuration();
								    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
								    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
									fs = FileSystem.get(conf);
								} catch (IOException e1) {
									e1.printStackTrace();
								}
								HashSet<BitSet> skipset = new HashSet<BitSet>();
								List<Package> l1, l2;
								
								if (l_list.isEmpty() &&
								   s_list.isEmpty() &&
								   r_list.isEmpty()) {
								   //System.out.println("Current level: " + b_cur_level.value() + "\t" +
								   //                   "Running time is " + 0 + ". All lists are empty.");
								   return; // pkgs is empty
								}else if(s_list.isEmpty()){// self is empty
								   l1 = l_list;
								   l2 = r_list;
								}else{
								   l1 = s_list;
								   l2 = s_list;
								}
								   
								// have big and small lists, read small once and cache, 
								// read big from disk one element at a time.
								List<Package> l_big, l_small;
								if(l1.size() < l2.size()) {
								   l_small = l1;
								   l_big = l2;
								}
								else {
								   l_small = l2;
								   l_big = l1;
								}
								   
								// cache the small list
								Hashtable<BitSet, StrippedPartition> l_small_cache = new Hashtable<BitSet, StrippedPartition>();
								for (Package pkg2 : l_small) {
									BitSet l2_bitset = pkg2.bitset;
								    String pkg2_combination_name = l2_bitset.toString();
								    pkg2_combination_name = pkg2_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
								    StrippedPartition pkg2_sp = null;
								   
								   try {
									   FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/tane_nocache/" + (b_cur_level.value()-1)+"_"+pkg2_combination_name));
									   BufferedReader br=new BufferedReader(new InputStreamReader(in));
									   
									   pkg2_sp = gson.fromJson(br, StrippedPartition.class);
									   br.close();
									   in.close();
								   } catch (IllegalArgumentException e) {
									   e.printStackTrace();
								   } catch (IOException e) {
									   e.printStackTrace();
								   }
								   System.out.println("... blah: "+ l2_bitset+"   "+ pkg2_sp.getStrippedPartition());
							       l_small_cache.put(l2_bitset, pkg2_sp);
								}
								   
								for (Package pkg1 : l_big) {
									BitSet l1_bitset = pkg1.bitset;
									String pkg1_combination_name = l1_bitset.toString();
									pkg1_combination_name = pkg1_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
									StrippedPartition pkg1_sp = null;
									try {
										FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/tane_nocache/"+ (b_cur_level.value()-1)+"_"+pkg1_combination_name));
										BufferedReader br=new BufferedReader(new InputStreamReader(in));
										//Gson gson = new Gson();
										pkg1_sp = gson.fromJson(br, StrippedPartition.class);
										br.close();
										in.close();
									} catch (IllegalArgumentException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}
								   
									for (Package pkg2 : l_small) {
										Hashtable<BitSet, CombinationHelper> htable = new Hashtable<BitSet, CombinationHelper>();
										BitSet l2_bitset = pkg2.bitset;
										BitSet l1orl2 = (BitSet)l1_bitset.clone();
										l1orl2.or(l2_bitset);
										StrippedPartition pkg2_sp = l_small_cache.get(l2_bitset);
								     
										if (!pkg1.prefix_index.equals(pkg2.prefix_index) ||
										       l1_bitset.equals(l2_bitset) ||
										       (l1orl2.cardinality() - l1_bitset.cardinality() != 1) ||
										       (l1orl2.cardinality() - l2_bitset.cardinality() != 1)) {
										    if(!pkg1.prefix_index.equals(pkg2.prefix_index)) {
										      continue;
										   }
										   if(l1_bitset.equals(l2_bitset)){
										      continue;
										   }
										   if((l1orl2.cardinality() - l1_bitset.cardinality() != 1)) {
										      continue;
										   }
										   if((l1orl2.cardinality() - l2_bitset.cardinality() != 1) ) {
										      continue;
										   }
										}
										if (skipset.contains(l1orl2)) {
										       continue;
										}else{
										       skipset.add(l1orl2);
										}
										combination_counter++;
										boolean xIsValid = true;
										// clone of X for usage in the following loop
										BitSet Xclone = (BitSet) l1orl2.clone();
										// print_dependency2(Xclone, "original: ");
										for (int l = l1orl2.nextSetBit(0); l >= 0; l = l1orl2.nextSetBit(l + 1)) {
											Xclone.clear(l);
											if (!b_level0_keyset.value().contains(Xclone)) {
												// print_dependency2(Xclone, "check: false : ");
												xIsValid = false;
												break;
											}else{
												// print_dependency2(Xclone, "check: true : ");
											}
											Xclone.set(l);
										}
								
										if (!xIsValid) {
											continue;
										}
								
										after_checksubset++;
								
								
										StrippedPartition st = null;
										CombinationHelper combinationhelper = new CombinationHelper();
										if (pkg1.ch.isValid() && pkg2.ch.isValid()){
											Vector<Integer> tTable = new Vector<Integer>(b_numberTuples.value());
								
											for (long i = 0; i < b_numberTuples.value(); i++) tTable.add(-1);
								
											// =============== multiply =========================
											multiply_times++;
											List<List<Integer>> result = new ArrayList<List<Integer>>();
											List<List<Integer>> pt1List = (List)(((ArrayList)pkg1_sp.getStrippedPartition()).clone());
											List<List<Integer>> pt2List = (List)(((ArrayList)pkg2_sp.getStrippedPartition()).clone());
											
											List<List<Integer>> partition = new ArrayList<List<Integer>>();
											long noOfElements = 0;
											// System.out.println(l1_bitset.toString() + l2_bitset.toString());
											// iterate over first stripped partition and fill tTable.
											for (Integer i = 0; i < pt1List.size(); i++) {
											   Integer lst_size = pt1List.get(i).size();
											   //System.out.println(" -- STATS: sub-list-1 min max: " + pt1List.get(i).get(0)+ " "+ pt1List.get(i).get(lst_size-1));
											   Integer j = 0;
											   while(j < lst_size){
										          tTable.set(pt1List.get(i).get(j), i);
										          j++;
											   }
											
											   partition.add(new ArrayList<Integer>());
											}
											
											// iterate over second stripped partition.
											for (Integer i = 0; i < pt2List.size(); i++) {
											   Integer lst_size = pt2List.get(i).size();
											   //System.out.println(" -- STATS: sub-list-2 min max: " + pt2List.get(i).get(0)+ " "+ pt2List.get(i).get(lst_size-1));
											   Integer j = 0;
											   while(j < lst_size){
											      Integer t_id = pt2List.get(i).get(j);
											
											      if (tTable.get(t_id) != -1) {
											         partition.get(tTable.get(t_id)).add(t_id);
											      }
											      j++;
											   }
											
											   j = 0;
											   while(j < lst_size){
											      Integer tId = pt2List.get(i).get(j);
											      // if condition not in the paper;
									              if (tTable.get(tId) != -1) {
									                 if (partition.get(tTable.get(tId)).size() > 1) {
									                    List<Integer> eqClass = partition.get(tTable.get(tId));
									                    result.add(eqClass);
									                    noOfElements += eqClass.size();
									                 }
									                 partition.set(tTable.get(tId), new ArrayList<Integer>());
									              }
									              j++;
									           }
									        }
											// =============== end pf multiply =========================
											st = new StrippedPartition(result, noOfElements);
								
										}else{
											combinationhelper.setInvalid();
										}
								
										BitSet rhsCandidates = new BitSet();
										
										//combinationhelper.setPartition(st);
										combinationhelper.setRhsCandidates(rhsCandidates);
										if(st != null) {
											combinationhelper.setElementCount(st.getElementCount());
											combinationhelper.setError(st.getError());
										}
								
										htable.put(l1orl2, combinationhelper);
										
										propertiesAccumulator.add(htable);
										// htable.clear();
										String combination_name = l1orl2.toString();
										combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
										if(st != null) {
											try {
												FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/tane_nocache/" + b_cur_level.value()+"_"+combination_name));
												BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
												//Gson gson = new Gson();
												br.write(gson.toJson(st));
												br.close();
												out.close();
											} catch (IllegalArgumentException e) {
												e.printStackTrace();
											} catch (IOException e) {
												e.printStackTrace();
											}
										}
										st = null;
									}
									pkg1_sp = null;
								}
                                 
                            }
                     });

                     foreach_rdd += (System.nanoTime() - foreach_rdd_start);

                     Long mapTime = System.nanoTime();
                     Hashtable<BitSet, CombinationHelper> new_level = propertiesAccumulator.value();
                     my_print("Generate next level time " + 0.001* (System.nanoTime() - broadcast_package_rdd_start)/1000000 + "sec");
                     level1 = new_level;
              }	


              private static void generateNextLevel_baseline_1( Integer cur_level) {
					level0 = level1;
					level1 = null;
					
					/*//Java serializatioin
 					ByteArrayOutputStream bos = new ByteArrayOutputStream();
 					ObjectOutput out = null;
 					try {
 					  out = new ObjectOutputStream(bos);
 					  out.writeObject(level0);
 					  out.flush();
 					  byte[] yourBytes2 = bos.toByteArray();
 					  System.out.println("Java serialization level0 length in bytes " + yourBytes2.length);
 					} catch (IOException e) {
 						e.printStackTrace();
 					} finally {
 		  				try {
 					  bos.close();
 						} catch (IOException ex) {
 					    // ignore close exception
 					  }
 					}*/
 					
					Long start = System.nanoTime();
					buildPrefixBlocks();
					Hashtable<String, Integer> all_combinations = new Hashtable<String, Integer>();
                    for (List<BitSet> prefix_block_list : prefix_blocks.values()) {
                       // continue only, if the prefix_block contains at least 2 elements
                       if (prefix_block_list.size() < 2) {
                          continue;
                       }
                       List<BitSet[]> combinations = getListCombinations(prefix_block_list);
                       for (BitSet[] c : combinations) {
                          BitSet X = (BitSet) c[0].clone();
                          X.or(c[1]);
                          if (checkSubsets(X))
                        	  all_combinations.put(X.toString(), 1);
                       }
                    }
                    
					final Broadcast<Integer> b_numberTuples = sc.broadcast(numberTuples);
					final Broadcast<Hashtable<String, Integer>> b_all_combinations = sc.broadcast(all_combinations);
					final Accumulator<Hashtable<BitSet, CombinationHelper>> propertiesAccumulator =
											sc.accumulator(new Hashtable<BitSet, CombinationHelper>(),
					                                        new BitSetAndCombinaitonHelperMapAccumulatorParam());
					
                    List<Tuple2<BitSet,String>> list1 = new ArrayList<Tuple2<BitSet,String>>();
                    Set<BitSet> keys = level0.keySet();
                    Gson gson = new Gson();
                    for(BitSet c : keys) {
                    	CombinationHelper ch = level0.get(c);
                    	String sp_str = gson.toJson(ch);
                    	list1.add(new Tuple2<BitSet,String>(c, sp_str));
                    }
                    Long join_start = System.nanoTime();
                    JavaPairRDD<BitSet,String> RDD1 = sc.parallelizePairs(list1);
                    JavaPairRDD<BitSet,String> RDD2 = sc.parallelizePairs(list1);
                    
                    JavaPairRDD<Tuple2<BitSet,String>, Tuple2<BitSet,String>> joinRDD = RDD1.cartesian(RDD2);
                    //System.out.println("Count: "+joinRDD.count());
                    Long join_end = System.nanoTime();
                    System.out.println(" Spark join time: " + 0.001* (join_end - join_start)/1000000);
                    
                    joinRDD.foreach(new VoidFunction<Tuple2< Tuple2<BitSet,String>, Tuple2<BitSet,String> >>(){
	                         // @Override
					public void call(Tuple2< Tuple2<BitSet,String>, Tuple2<BitSet,String> > c_list) {
						
						Tuple2<BitSet,String> e1 = c_list._1;
						Tuple2<BitSet,String> e2 = c_list._2;
						
						BitSet c1_bitset = e1._1;
						BitSet c2_bitset = e2._1;		
						BitSet X = (BitSet) c1_bitset.clone();
						X.or(c2_bitset);
						
						Boolean valid = false;
						if(b_all_combinations.value().containsKey(X.toString()))
							valid = true;
						if(!valid)
							return;
						
						Gson gson = new Gson();
						CombinationHelper pkg1_sp = gson.fromJson(e1._2, CombinationHelper.class);
						CombinationHelper pkg2_sp = gson.fromJson(e2._2, CombinationHelper.class); 
						StrippedPartition st = null;
						CombinationHelper ch = new CombinationHelper();
						
						if (pkg1_sp.isValid() && pkg2_sp.isValid()) {
						    List<Integer> local_tTable = new ArrayList<Integer>(b_numberTuples.value());
						    for (long i = 0; i < b_numberTuples.value(); i++) {
						       local_tTable.add(-1);
						    }
						
							// =============== multiply =========================
							List<List<Integer>> result = new ArrayList<List<Integer>>();
							List<List<Integer>> pt1List = (List)(((ArrayList)pkg1_sp.getPartition().getStrippedPartition()).clone());
							List<List<Integer>> pt2List = (List)(((ArrayList)pkg2_sp.getPartition().getStrippedPartition()).clone());
							List<List<Integer>> partition = new ArrayList<List<Integer>>();
							long noOfElements = 0;
							
							// iterate over first stripped partition and fill tTable.
							for (Integer i = 0; i < pt1List.size(); i++) {
							   Integer lst_size = pt1List.get(i).size();
							   Integer j = 0;
							   while(j < lst_size){
							          local_tTable.set(pt1List.get(i).get(j), i);
							          j++;
							   }
							   partition.add(new ArrayList<Integer>());
							}
							
							for (Integer i = 0; i < pt2List.size(); i++) {
							   Integer lst_size = pt2List.get(i).size();
							   Integer j = 0;
							   while(j < lst_size){
							      Integer t_id = pt2List.get(i).get(j);
							      if (local_tTable.get(t_id) != -1) {
							         partition.get(local_tTable.get(t_id)).add(t_id);
							      }
							      j++;
							   }
							
							   j = 0;
							   while(j < lst_size){
							      Integer tId = pt2List.get(i).get(j);
							      // if condition not in the paper;
							      if (local_tTable.get(tId) != -1) {
							         if (partition.get(local_tTable.get(tId)).size() > 1) {
							            List<Integer> eqClass = partition.get(local_tTable.get(tId));
							            result.add(eqClass);
							            noOfElements += eqClass.size();
							         }
							         partition.set(local_tTable.get(tId), new ArrayList<Integer>());
							      }
							      j++;
							   }
							}
							// =============== end of multiply =========================
							st = new StrippedPartition(result, noOfElements);
						} else {
						   ch.setInvalid();
						}
						BitSet rhsCandidates = new BitSet();
						ch.setPartition(st);
						ch.setRhsCandidates(rhsCandidates);
						if(st != null) {
							ch.setElementCount(st.getElementCount());
							ch.setError(st.getError());
						}
						Hashtable<BitSet, CombinationHelper> htable = new Hashtable<BitSet, CombinationHelper>();
						htable.put(X, ch);
						propertiesAccumulator.add(htable);
						
						// What to do with "st"
                  }
                  });
					Hashtable<BitSet, CombinationHelper> new_level = propertiesAccumulator.value();
					my_print("multiplication time " + 0.001* (System.nanoTime() - join_end)/1000000 + "sec");
					my_print("Generate next level time " + 0.001* (System.nanoTime() - start)/1000000 + "sec");
					level1 = new_level;
            }	
              
              private static void generateNextLevel_baseline_2a( Integer cur_level) {
					level0 = level1;
					level1 = null;
                  
					long t1 = System.currentTimeMillis();
					Long start = System.nanoTime();
					buildPrefixBlocks();
                  //List<BitSet[]> all_combinations = new ArrayList<BitSet[]>();
                  Hashtable<BitSet, Integer> all_combinations = new Hashtable<BitSet, Integer>();
                  for (List<BitSet> prefix_block_list : prefix_blocks.values()) {
                         // continue only, if the prefix_block contains at least 2 elements
                         if (prefix_block_list.size() < 2) {
                                continue;
                         }
                         List<BitSet[]> combinations = getListCombinations(prefix_block_list);
                         for (BitSet[] c : combinations) {
                                BitSet X = (BitSet) c[0].clone();
                                X.or(c[1]);
                                if (checkSubsets(X))
                                       //all_combinations.add(c);
                                	all_combinations.put(X, 1);
                         }
                  }
                  
                  	final Broadcast< Hashtable<BitSet, CombinationHelper> > b_level0 = sc.broadcast(level0);
					final Broadcast<Integer> b_numberTuples = sc.broadcast(numberTuples);
					//final Broadcast<List<BitSet[]>> b_all_combinations = sc.broadcast(all_combinations);
					final Broadcast<Hashtable<BitSet, Integer>> b_all_combinations = sc.broadcast(all_combinations);
					final Broadcast<Integer> b_cur_level = sc.broadcast(cur_level);
					
					
                  Set<BitSet> keys = level0.keySet();
                  List<BitSet> keys_list = new ArrayList(keys);
                	  final Accumulator<Hashtable<BitSet, CombinationHelper>> propertiesAccumulator =
								sc.accumulator(new Hashtable<BitSet, CombinationHelper>(),
		                                        new BitSetAndCombinaitonHelperMapAccumulatorParam());
                	  
	                  JavaRDD<BitSet> keysRDD = sc.parallelize(keys_list, numberExecutors); //  
	                  // Read data from HDFS and create pairRDD
	                  JavaPairRDD<BitSet,StrippedPartition> level0RDD = keysRDD.mapToPair(new PairFunction<BitSet, BitSet, StrippedPartition>() {
	                	  @Override
	                	  public Tuple2<BitSet,StrippedPartition> call(BitSet c) throws Exception {
								String pkg1_combination_name = c.toString();
								pkg1_combination_name = pkg1_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
								StrippedPartition pkg1_st = null;
								try {
									Configuration conf = new Configuration();
								    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
								    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
									FileSystem fs = FileSystem.get(conf);
									FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/tane_nocache/"+ (b_cur_level.value()-1)+"_"+pkg1_combination_name));
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
	                	    return new Tuple2<BitSet, StrippedPartition>(c, pkg1_st);
	                	  }
	                	});
	                  
	                  JavaPairRDD<Tuple2<BitSet,StrippedPartition>, Tuple2<BitSet,StrippedPartition>> joinRDD = level0RDD.cartesian(level0RDD).repartition(numberExecutors);
	                  //System.out.println("Count: "+joinRDD.count());
	                  
	                  long t2 = System.currentTimeMillis();
	                  latticeGenTime += t2-t1;
	                  
	                  joinRDD.foreach(new VoidFunction<Tuple2< Tuple2<BitSet,StrippedPartition>, Tuple2<BitSet,StrippedPartition> >>(){
	                         // @Override
						public void call(Tuple2< Tuple2<BitSet,StrippedPartition>, Tuple2<BitSet,StrippedPartition> > c_list) {
							
							Tuple2<BitSet,StrippedPartition> e1 = c_list._1;
							Tuple2<BitSet,StrippedPartition> e2 = c_list._2;
							
							BitSet c1_bitset = e1._1;
							BitSet c2_bitset = e2._1;		
							
							BitSet X = (BitSet) c1_bitset.clone();
							X.or(c2_bitset);
							
							Boolean valid = false;
							if(b_all_combinations.value().containsKey(X))
								valid = true;
							/*for(BitSet[] c : b_all_combinations.value()) {
								if(c1_bitset.equals(c[0]) && c2_bitset.equals(c[1]))
									valid = true;
							}*/
							if(!valid)
								return;
							
							Gson gson = new Gson();
							StrippedPartition pkg1_sp = e1._2;
							StrippedPartition pkg2_sp = e2._2; 
							
							StrippedPartition st = null;
							CombinationHelper ch = new CombinationHelper();
							
							if (b_level0.value().get(c1_bitset).isValid() && b_level0.value().get(c2_bitset).isValid()) {
							    List<Integer> local_tTable = new ArrayList<Integer>(b_numberTuples.value());
							    for (long i = 0; i < b_numberTuples.value(); i++) {
							       local_tTable.add(-1);
							    }
							
								// =============== multiply =========================
								List<List<Integer>> result = new ArrayList<List<Integer>>();
								List<List<Integer>> pt1List = (List)(((ArrayList)pkg1_sp.getStrippedPartition()).clone());
								List<List<Integer>> pt2List = (List)(((ArrayList)pkg2_sp.getStrippedPartition()).clone());
								List<List<Integer>> partition = new ArrayList<List<Integer>>();
								long noOfElements = 0;
								
								// iterate over first stripped partition and fill tTable.
								for (Integer i = 0; i < pt1List.size(); i++) {
								   Integer lst_size = pt1List.get(i).size();
								   Integer j = 0;
								   while(j < lst_size){
								          local_tTable.set(pt1List.get(i).get(j), i);
								          j++;
								   }
								   partition.add(new ArrayList<Integer>());
								}
								
								for (Integer i = 0; i < pt2List.size(); i++) {
								   Integer lst_size = pt2List.get(i).size();
								   Integer j = 0;
								   while(j < lst_size){
								      Integer t_id = pt2List.get(i).get(j);
								      if (local_tTable.get(t_id) != -1) {
								         partition.get(local_tTable.get(t_id)).add(t_id);
								      }
								      j++;
								   }
								
								   j = 0;
								   while(j < lst_size){
								      Integer tId = pt2List.get(i).get(j);
								      // if condition not in the paper;
								      if (local_tTable.get(tId) != -1) {
								         if (partition.get(local_tTable.get(tId)).size() > 1) {
								            List<Integer> eqClass = partition.get(local_tTable.get(tId));
								            result.add(eqClass);
								            noOfElements += eqClass.size();
								         }
								         partition.set(local_tTable.get(tId), new ArrayList<Integer>());
								      }
								      j++;
								   }
								}
								// =============== end of multiply =========================
								st = new StrippedPartition(result, noOfElements);
							} else {
							   ch.setInvalid();
							}
							BitSet rhsCandidates = new BitSet();
							//ch.setPartition(st);
							ch.setRhsCandidates(rhsCandidates);
							if(st != null) {
								ch.setElementCount(st.getElementCount());
								ch.setError(st.getError());
							}
							Hashtable<BitSet, CombinationHelper> htable = new Hashtable<BitSet, CombinationHelper>();
							htable.put(X, ch);
							propertiesAccumulator.add(htable);
							String combination_name = X.toString();
							combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
							
							if(st != null) {
								try {
									Configuration conf = new Configuration();
								    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
								    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
									FileSystem fs = FileSystem.get(conf);
									FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/tane_nocache/" + b_cur_level.value()+"_"+combination_name));
									BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
									//Gson gson = new Gson();
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
		                  });
	                  /*for(Entry<BitSet, CombinationHelper> e : propertiesAccumulator.value().entrySet()) {
	                	  new_level.put(e.getKey(), e.getValue());
	                  }*/
                  //}
                  
                  Hashtable<BitSet, CombinationHelper> new_level = propertiesAccumulator.value();
					my_print("Generate next level time " + 0.001* (System.nanoTime() - start)/1000000 + "sec");
					level1 = new_level;
					long t3 = System.currentTimeMillis();
					genEQClassTime += t3-t2;
          }
              
              private static void generateNextLevel_triangle( Integer cur_level) {
					level0 = level1;
					level1 = null;
                
					long t1 = System.currentTimeMillis();
					Long start = System.nanoTime();
					buildPrefixBlocks();
                //List<BitSet[]> all_combinations = new ArrayList<BitSet[]>();
                Hashtable<BitSet, Integer> all_combinations = new Hashtable<BitSet, Integer>();
                for (List<BitSet> prefix_block_list : prefix_blocks.values()) {
                       // continue only, if the prefix_block contains at least 2 elements
                       if (prefix_block_list.size() < 2) {
                              continue;
                       }
                       List<BitSet[]> combinations = getListCombinations(prefix_block_list);
                       for (BitSet[] c : combinations) {
                              BitSet X = (BitSet) c[0].clone();
                              X.or(c[1]);
                              if (checkSubsets(X))
                                     //all_combinations.add(c);
                              	all_combinations.put(X, 1);
                       }
                }
                
                	final Broadcast< Hashtable<BitSet, CombinationHelper> > b_level0 = sc.broadcast(level0);
					final Broadcast<Integer> b_numberTuples = sc.broadcast(numberTuples);
					//final Broadcast<List<BitSet[]>> b_all_combinations = sc.broadcast(all_combinations);
					final Broadcast<Hashtable<BitSet, Integer>> b_all_combinations = sc.broadcast(all_combinations);
					final Broadcast<Integer> b_cur_level = sc.broadcast(cur_level);
					final Broadcast<Integer> b_sideLen = sc.broadcast(sideLen);
					
					
                Set<BitSet> keys = level0.keySet();
                List<BitSet> keys_list = new ArrayList(keys);
              	  final Accumulator<Hashtable<BitSet, CombinationHelper>> propertiesAccumulator =
								sc.accumulator(new Hashtable<BitSet, CombinationHelper>(),
		                                        new BitSetAndCombinaitonHelperMapAccumulatorParam());
              	  
	                  JavaRDD<BitSet> keysRDD = sc.parallelize(keys_list, numberExecutors); //  
	                  // Read data from HDFS and create pairRDD
	                  JavaPairRDD<BitSet,StrippedPartition> level0RDD = keysRDD.mapToPair(new PairFunction<BitSet, BitSet, StrippedPartition>() {
	                	  @Override
	                	  public Tuple2<BitSet,StrippedPartition> call(BitSet c) throws Exception {
								String pkg1_combination_name = c.toString();
								pkg1_combination_name = pkg1_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
								StrippedPartition pkg1_st = null;
								try {
									Configuration conf = new Configuration();
								    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
								    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
									FileSystem fs = FileSystem.get(conf);
									FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/tane_nocache/"+ (b_cur_level.value()-1)+"_"+pkg1_combination_name));
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
	                	    return new Tuple2<BitSet, StrippedPartition>(c, pkg1_st);
	                	  }
	                	});
	                  
	                  // DEBUG
	                  /*Map<BitSet, StrippedPartition> mp1 = level0RDD.collectAsMap();
	                  for(BitSet b : mp1.keySet())
	                	  System.out.println(b);
	                  System.out.println();*/
	                  
	                  // Assign rows to L, s, R executors
	                  JavaPairRDD<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>> rowMap = level0RDD.flatMapToPair(new PairFlatMapFunction<Tuple2<BitSet,StrippedPartition>, Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>>() {
	              		public Iterator<Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>>> call(Tuple2<BitSet,StrippedPartition> r) {
	              			List<Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>>> result = new ArrayList<Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>>>();
	              			int a = (int)(Math.random()*b_sideLen.value()) + 1;
	              			for(int p = 1; p < a; p++) {
	              				int rid = getReducerId(p, a);
	              				Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>> t = 
	              						new Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>>(rid, new Tuple2<String, Tuple2<BitSet,StrippedPartition>>("L", r));
	              				result.add(t);
	              			}
	              			int rid2 = getReducerId(a, a);
	              			Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>> t2 = 
	          						new Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>>(rid2, new Tuple2<String, Tuple2<BitSet,StrippedPartition>>("S", r));
	          				result.add(t2);
	          				for(int q = a+1; q <= b_sideLen.value(); q++) {
	          					int rid = getReducerId(a, q);
	          					Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>> t = 
	              						new Tuple2<Integer, Tuple2<String, Tuple2<BitSet,StrippedPartition>>>(rid, new Tuple2<String, Tuple2<BitSet,StrippedPartition>>("R", r));
	              				result.add(t);
	          				}
	              			return result.iterator();	
	              		}
	              	});
	                  
	                  JavaPairRDD<Integer, Iterable<Tuple2<String, Tuple2<BitSet,StrippedPartition>>>> pairsPartition = rowMap.partitionBy(new Partitioner() {
	                      @Override
	                      public int getPartition(Object key) {
	                          return (int) key - 1;
	                      }

	                      @Override
	                      public int numPartitions() {
	                          return 55;
	                      }
	                  }).groupByKey();
	                  
	                  // DEBUG
	                  /*Map<Integer, Iterable<Tuple2<String, Tuple2<BitSet,StrippedPartition>>> > mp2 = pairsPartition.collectAsMap();
	                  for(Integer i : mp2.keySet()){
	                	  System.out.println(i);
	                	  for(Tuple2<String, Tuple2<BitSet,StrippedPartition>> t : mp2.get(i)) {
	                		  System.out.println( t._1+ " "+t._2._1);
	                	  }
	                  }
	                  System.out.println();*/
	                  
	                  pairsPartition.foreach(new VoidFunction<Tuple2< Integer, Iterable<Tuple2< String, Tuple2<BitSet, StrippedPartition> >>> > () {
	                	  public void call(Tuple2<Integer, Iterable<Tuple2<String, Tuple2<BitSet, StrippedPartition>>>> t) {
	                		  List<Tuple2<BitSet, StrippedPartition>> left = new ArrayList<Tuple2<BitSet, StrippedPartition>>();
	      					List<Tuple2<BitSet, StrippedPartition>> right = new ArrayList<Tuple2<BitSet, StrippedPartition>>();
	      					List<Tuple2<BitSet, StrippedPartition>> self = new ArrayList<Tuple2<BitSet, StrippedPartition>>();
	      					for(Tuple2<String, Tuple2<BitSet, StrippedPartition>> rt: t._2) {
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
							Hashtable<BitSet, CombinationHelper> htable = new Hashtable<BitSet, CombinationHelper>();
							
	      					if(left.size() != 0 && right.size() != 0) {
	    						for(Tuple2<BitSet, StrippedPartition> e1 : left) {
	        						for(Tuple2<BitSet, StrippedPartition> e2 : right) {
	        							BitSet c1_bitset = e1._1;
	        							BitSet c2_bitset = e2._1;		
	        							
	        							BitSet X = (BitSet) c1_bitset.clone();
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
	        							    List<Integer> local_tTable = new ArrayList<Integer>(b_numberTuples.value());
	        							    for (long i = 0; i < b_numberTuples.value(); i++) {
	        							       local_tTable.add(-1);
	        							    }
	        							
	        								// =============== multiply =========================
	        								List<List<Integer>> result = new ArrayList<List<Integer>>();
	        								List<List<Integer>> pt1List = (List)(((ArrayList)pkg1_sp.getStrippedPartition()).clone());
	        								List<List<Integer>> pt2List = (List)(((ArrayList)pkg2_sp.getStrippedPartition()).clone());
	        								List<List<Integer>> partition = new ArrayList<List<Integer>>();
	        								long noOfElements = 0;
	        								
	        								// iterate over first stripped partition and fill tTable.
	        								for (Integer i = 0; i < pt1List.size(); i++) {
	        								   Integer lst_size = pt1List.get(i).size();
	        								   Integer j = 0;
	        								   while(j < lst_size){
	        								          local_tTable.set(pt1List.get(i).get(j), i);
	        								          j++;
	        								   }
	        								   partition.add(new ArrayList<Integer>());
	        								}
	        								
	        								for (Integer i = 0; i < pt2List.size(); i++) {
	        								   Integer lst_size = pt2List.get(i).size();
	        								   Integer j = 0;
	        								   while(j < lst_size){
	        								      Integer t_id = pt2List.get(i).get(j);
	        								      if (local_tTable.get(t_id) != -1) {
	        								         partition.get(local_tTable.get(t_id)).add(t_id);
	        								      }
	        								      j++;
	        								   }
	        								
	        								   j = 0;
	        								   while(j < lst_size){
	        								      Integer tId = pt2List.get(i).get(j);
	        								      // if condition not in the paper;
	        								      if (local_tTable.get(tId) != -1) {
	        								         if (partition.get(local_tTable.get(tId)).size() > 1) {
	        								            List<Integer> eqClass = partition.get(local_tTable.get(tId));
	        								            result.add(eqClass);
	        								            noOfElements += eqClass.size();
	        								         }
	        								         partition.set(local_tTable.get(tId), new ArrayList<Integer>());
	        								      }
	        								      j++;
	        								   }
	        								}
	        								// =============== end of multiply =========================
	        								st = new StrippedPartition(result, noOfElements);
	        							} else {
	        							   ch.setInvalid();
	        							}
	        							BitSet rhsCandidates = new BitSet();
	        							//ch.setPartition(st);
	        							ch.setRhsCandidates(rhsCandidates);
	        							if(st != null) {
	        								ch.setElementCount(st.getElementCount());
	        								ch.setError(st.getError());
	        							}
	        							
	        							htable.put(X, ch);
	        							//propertiesAccumulator.add(htable);
	        							String combination_name = X.toString();
	        							combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
	        							if(st != null) {
	        								try {
	        									FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/tane_nocache/" + b_cur_level.value()+"_"+combination_name));
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
	    						for(Tuple2<BitSet, StrippedPartition> e1 : self) {
	        						for(Tuple2<BitSet, StrippedPartition> e2 : self) {
	        							BitSet c1_bitset = e1._1;
	        							BitSet c2_bitset = e2._1;		
	        							
	        							BitSet X = (BitSet) c1_bitset.clone();
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
	        							    List<Integer> local_tTable = new ArrayList<Integer>(b_numberTuples.value());
	        							    for (long i = 0; i < b_numberTuples.value(); i++) {
	        							       local_tTable.add(-1);
	        							    }
	        							
	        								// =============== multiply =========================
	        								List<List<Integer>> result = new ArrayList<List<Integer>>();
	        								List<List<Integer>> pt1List = (List)(((ArrayList)pkg1_sp.getStrippedPartition()).clone());
	        								List<List<Integer>> pt2List = (List)(((ArrayList)pkg2_sp.getStrippedPartition()).clone());
	        								List<List<Integer>> partition = new ArrayList<List<Integer>>();
	        								long noOfElements = 0;
	        								
	        								// iterate over first stripped partition and fill tTable.
	        								for (Integer i = 0; i < pt1List.size(); i++) {
	        								   Integer lst_size = pt1List.get(i).size();
	        								   Integer j = 0;
	        								   while(j < lst_size){
	        								          local_tTable.set(pt1List.get(i).get(j), i);
	        								          j++;
	        								   }
	        								   partition.add(new ArrayList<Integer>());
	        								}
	        								
	        								for (Integer i = 0; i < pt2List.size(); i++) {
	        								   Integer lst_size = pt2List.get(i).size();
	        								   Integer j = 0;
	        								   while(j < lst_size){
	        								      Integer t_id = pt2List.get(i).get(j);
	        								      if (local_tTable.get(t_id) != -1) {
	        								         partition.get(local_tTable.get(t_id)).add(t_id);
	        								      }
	        								      j++;
	        								   }
	        								
	        								   j = 0;
	        								   while(j < lst_size){
	        								      Integer tId = pt2List.get(i).get(j);
	        								      // if condition not in the paper;
	        								      if (local_tTable.get(tId) != -1) {
	        								         if (partition.get(local_tTable.get(tId)).size() > 1) {
	        								            List<Integer> eqClass = partition.get(local_tTable.get(tId));
	        								            result.add(eqClass);
	        								            noOfElements += eqClass.size();
	        								         }
	        								         partition.set(local_tTable.get(tId), new ArrayList<Integer>());
	        								      }
	        								      j++;
	        								   }
	        								}
	        								// =============== end of multiply =========================
	        								st = new StrippedPartition(result, noOfElements);
	        							} else {
	        							   ch.setInvalid();
	        							}
	        							BitSet rhsCandidates = new BitSet();
	        							//ch.setPartition(st);
	        							ch.setRhsCandidates(rhsCandidates);
	        							if(st != null) {
	        								ch.setElementCount(st.getElementCount());
	        								ch.setError(st.getError());
	        							}
	        							//Hashtable<BitSet, CombinationHelper> htable = new Hashtable<BitSet, CombinationHelper>();
	        							htable.put(X, ch);
	        							//propertiesAccumulator.add(htable);
	        							String combination_name = X.toString();
	        							combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
	        							if(st != null) {
	        								try {
	        									FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/tane_nocache/" + b_cur_level.value()+"_"+combination_name));
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
	      					propertiesAccumulator.add(htable);
	                	  }
	                  });
	                  
                Hashtable<BitSet, CombinationHelper> new_level = propertiesAccumulator.value();
					my_print("Generate next level time " + 0.001* (System.nanoTime() - start)/1000000 + "sec");
					level1 = new_level;
					long t2 = System.currentTimeMillis();
					genEQClassTime += t2-t1;
        }
              
              private static int getReducerId(int i, int j) {
              	int t1 = (2*sideLen - i + 2)*(i-1)/2;
              	int t2 = j - i + 1;
              	return t1 + t2;
              }
              
              
              private static void generateNextLevel_broadcast(Integer cur_level) {
                     level0 = level1;
                     level1 = null;
                     Long startTime = System.nanoTime();

                     //System.gc();

                     buildPrefixBlocks();

                     Integer counter = 0;
                     List<BitSet[]> all_combinations = new ArrayList<BitSet[]>();

                     for (List<BitSet> prefix_block_list : prefix_blocks.values()) {

                            // continue only, if the prefix_block contains at least 2 elements
                            if (prefix_block_list.size() < 2) {
                                   continue;
                            }

                            List<BitSet[]> combinations = getListCombinations(prefix_block_list);

                            for (BitSet[] c : combinations) {
                                   BitSet X = (BitSet) c[0].clone();
                                   X.or(c[1]);
                                   if (checkSubsets(X))
                                          all_combinations.add(c);
                            }
                     }

                     Long multiplyTime = System.nanoTime();

                     final Broadcast< Hashtable<BitSet, CombinationHelper> > b_level0 = sc.broadcast(level0);
                     final Broadcast<Integer> b_numberTuples = sc.broadcast(numberTuples);
                     final Broadcast<Integer> b_cur_level = sc.broadcast(cur_level);
                     final Accumulator<Hashtable<BitSet, CombinationHelper>> propertiesAccumulator =
                                   sc.accumulator(new Hashtable<BitSet, CombinationHelper>(),
                                                           new BitSetAndCombinaitonHelperMapAccumulatorParam());

                     my_print("Broadcasting " + 0.001* (System.nanoTime() - multiplyTime)/1000000 + "sec");
                     long total_integer_size = 0;
                     long total_list_size = 0;


                     Long rdd = System.nanoTime();
                     JavaRDD<BitSet[]> all_combinationsRDD = sc.parallelize(all_combinations);
                     my_print("Create RDD time" + 0.001* (System.nanoTime() - rdd)/1000000 + "sec");

                     all_combinationsRDD.foreach(new VoidFunction<BitSet[]>(){
                            // @Override
                            public void call(BitSet[] c) {
                                   BitSet X = (BitSet) c[0].clone();
                                   X.or(c[1]);
                                   StrippedPartition st = null;
                                   CombinationHelper ch = new CombinationHelper();

                                   b_level0.value();
                                   b_numberTuples.value();
                                   b_cur_level.value();
                                   //System.gc();

                                   long b1 = 0;
                                   long b2 = 0;
                                   long b3 = 0;
                                   long b4 = 0;

                                   long x1 = 0;
                                   long x11 = 0;
                                   long x2 = 0;
                                   long x21 = 0;
                                   long x3 = 0;
                                   long x31 = 0;

                                   long local_foreach_rdd_inside_start = System.nanoTime();

                                   if (b_level0.value().get(c[0]).isValid() && b_level0.value().get(c[1]).isValid()) {
                                          List<Integer> local_tTable = new ArrayList<Integer>(b_numberTuples.value());
                                          for (long i = 0; i < b_numberTuples.value(); i++) {
                                                 local_tTable.add(-1);
                                          }

                                          List<List<Integer>> result = new ArrayList<List<Integer>>();
                                          List<List<Integer>> pt1List = b_level0.value().get(c[0]).getPartition().getStrippedPartition();
                                          List<List<Integer>> pt2List = b_level0.value().get(c[1]).getPartition().getStrippedPartition();
                                          List<List<Integer>> partition = new ArrayList<List<Integer>>();
                                          long noOfElements = 0;

                                          // System.out.println("product " + c[0].toString() + "\t" + pt1List.size() + "\t" +  c[1].toString() + "\t" + pt2List.size());

                                          // iterate over first stripped partition and fill tTable.
                                          for (Integer i = 0; i < pt1List.size(); i++) {
                                                 Integer lst_size = pt1List.get(i).size();
                                                 Integer j = 0;
                                                 long x1_start = System.nanoTime();
                                                 while(j < lst_size){
                                                        long x11_start = System.nanoTime();
                                                        local_tTable.set(pt1List.get(i).get(j), i);
                                                        j++;
                                                        x11 += (System.nanoTime() - x11_start)/1000;
                                                 }
                                                 x1 += (System.nanoTime() - x1_start)/1000;

                                                 partition.add(new ArrayList<Integer>());
                                          }

                                          for (Integer i = 0; i < pt2List.size(); i++) {
                                                 Integer lst_size = pt2List.get(i).size();
                                                 Integer j = 0;

                                                 long x2_start = System.nanoTime();
                                                 while(j < lst_size){
                                                        long x21_start = System.nanoTime();
                                                        Integer t_id = pt2List.get(i).get(j);

                                                        if (local_tTable.get(t_id) != -1) {
                                                               partition.get(local_tTable.get(t_id)).add(t_id);
                                                        }
                                                        j++;
                                                        x21 += (System.nanoTime() - x21_start)/1000;

                                                 }
                                                 x2 += (System.nanoTime() - x2_start)/1000;

                                                 j = 0;
                                                 long x3_start = System.nanoTime();
                                                 while(j < lst_size){
                                                        long x31_start = System.nanoTime();

                                                        Integer tId = pt2List.get(i).get(j);
                                                        // if condition not in the paper;
                                                        if (local_tTable.get(tId) != -1) {
                                                               if (partition.get(local_tTable.get(tId)).size() > 1) {
                                                                      List<Integer> eqClass = partition.get(local_tTable.get(tId));
                                                                      result.add(eqClass);
                                                                      noOfElements += eqClass.size();
                                                               }
                                                               partition.set(local_tTable.get(tId), new ArrayList<Integer>());
                                                        }
                                                        j++;

                                                        x31 += (System.nanoTime() - x31_start)/1000;
                                                 }
                                                 x3 += (System.nanoTime() - x3_start)/1000;

                                          }

                                          st = new StrippedPartition(result, noOfElements);

                                   } else {
                                          ch.setInvalid();
                                   }
                                   BitSet rhsCandidates = new BitSet();

                                   //ch.setPartition(st);
                                   ch.setRhsCandidates(rhsCandidates);
                                   if(st != null){
                                	   ch.setElementCount(st.getElementCount());
                                	   ch.setError(st.getError());
                                   }

                                   Hashtable<BitSet, CombinationHelper> htable = new Hashtable<BitSet, CombinationHelper>();
                                   htable.put(X, ch);

                                   long local_foreach_rdd_inside = (System.nanoTime() - local_foreach_rdd_inside_start)/1000;
                                   System.out.println("Current level:\t" + b_cur_level.value() + "\t" +
                                                      "Running time is:\t" + 0.001 * local_foreach_rdd_inside + "ms\t" +
                                                      "Running time is:\t" + 0.001 * (local_foreach_rdd_inside + x11 - x1 + x21 - x2 + x31 - x3) + "ms\t"
                                                      + "\tb1: " + b1/1000000
                                                      + "\tb2: " + b2/1000000
                                                      + "\tb3: " + b3/1000000
                                                      + "\tb4: " + b4/1000000);
                                   propertiesAccumulator.add(htable);

                            }

                     });
                     Long mapTime = System.nanoTime();
                     Hashtable<BitSet, CombinationHelper> new_level = propertiesAccumulator.value();

                     // new_level.putAll(m);
                     my_print("hashtable time " + 0.001* (System.nanoTime() - mapTime)/1000000 + "sec");
                     my_print("Generate next level time " + 0.001* (System.nanoTime() - startTime)/1000000 + "sec");
                     level1 = new_level;
              }


              private static void generateNextLevel_lowSpace(Integer executor_number_per_row, Integer cur_level) {
                  level0 = level1;
                  level1 = null;
                  Long broadcast_package_rdd_start = System.nanoTime();

                  //package_partitions size is the same as the number of executors
                  List<Packages> package_partitions = create_package_bag(executor_number_per_row);

                   
                  final Broadcast<Integer> b_numberTuples = sc.broadcast(numberTuples);
                  final Broadcast<Integer> b_cur_level = sc.broadcast(cur_level);
                  
                  HashSet<BitSet> lo_keyset = new HashSet<BitSet>();
                  lo_keyset.addAll(level0.keySet());
                  final Broadcast<HashSet<BitSet>> b_level0_keyset = sc.broadcast(lo_keyset);

                  Integer parition_number = (executor_number_per_row * (1 + executor_number_per_row))/2;

                  Long rdd = System.nanoTime();

                  final Accumulator<Hashtable<BitSet, CombinationHelper>> propertiesAccumulator =
                                sc.accumulator(new Hashtable<BitSet, CombinationHelper>(),
                                                        new BitSetAndCombinaitonHelperMapAccumulatorParam());
                  
                  JavaRDD<Packages> partitionRDD = sc.parallelize(package_partitions, parition_number).cache();
                  my_print("Create RDD time" + 0.001* (System.nanoTime() - broadcast_package_rdd_start)/1000000 + "sec");

                  broadcast_package_rdd += (System.nanoTime() - broadcast_package_rdd_start);

                  Long foreach_rdd_start = System.nanoTime();
                  
                  partitionRDD.foreach(new VoidFunction<Packages>(){
                         // @Override
                         public void call(Packages pkgs) {
                         	
								List<Package> l_list = pkgs.l_list;
								List<Package> s_list = pkgs.s_list;
								List<Package> r_list = pkgs.r_list;
								
								FileSystem fs = null;
								Gson gson = new Gson();
								try {
									Configuration conf = new Configuration();
								    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
								    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
									fs = FileSystem.get(conf);
								} catch (IOException e1) {
									e1.printStackTrace();
								}
								HashSet<BitSet> skipset = new HashSet<BitSet>();
								List<Package> l1, l2;
								
								if (l_list.isEmpty() &&
								   s_list.isEmpty() &&
								   r_list.isEmpty()) {
								   //System.out.println("Current level: " + b_cur_level.value() + "\t" +
								   //                   "Running time is " + 0 + ". All lists are empty.");
								   return; // pkgs is empty
								}else if(s_list.isEmpty()){// self is empty
								   l1 = l_list;
								   l2 = r_list;
								}else{
								   l1 = s_list;
								   l2 = s_list;
								}
								   
								// have big and small lists, read small once and cache, 
								// read big from disk one element at a time.
								List<Package> l_big, l_small;
								if(l1.size() < l2.size()) {
								   l_small = l1;
								   l_big = l2;
								}
								else {
								   l_small = l2;
								   l_big = l1;
								}
								   
								// cache the small list
								Hashtable<BitSet, StrippedPartition> l_small_cache = new Hashtable<BitSet, StrippedPartition>();
								for (Package pkg2 : l_small) {
									BitSet l2_bitset = pkg2.bitset;
								    String pkg2_combination_name = l2_bitset.toString();
								    pkg2_combination_name = pkg2_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
								    StrippedPartition pkg2_sp = null;
								   
								   try {
									   FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/tane_nocache/" + (b_cur_level.value()-1)+"_"+pkg2_combination_name));
									   BufferedReader br=new BufferedReader(new InputStreamReader(in));
									   
									   pkg2_sp = gson.fromJson(br, StrippedPartition.class);
									   br.close();
									   in.close();
								   } catch (IllegalArgumentException e) {
									   e.printStackTrace();
								   } catch (IOException e) {
									   e.printStackTrace();
								   }
							       l_small_cache.put(l2_bitset, pkg2_sp);
								}
								   
								for (Package pkg1 : l_big) {
									BitSet l1_bitset = pkg1.bitset;
									String pkg1_combination_name = l1_bitset.toString();
									pkg1_combination_name = pkg1_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
									StrippedPartition pkg1_sp = null;
									try {
										FSDataInputStream in = fs.open(new Path("hdfs://husky-06:8020/tmp/tane_nocache/"+ (b_cur_level.value()-1)+"_"+pkg1_combination_name));
										BufferedReader br=new BufferedReader(new InputStreamReader(in));
										//Gson gson = new Gson();
										pkg1_sp = gson.fromJson(br, StrippedPartition.class);
										br.close();
										in.close();
									} catch (IllegalArgumentException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}
								   
									for (Package pkg2 : l_small) {
										Hashtable<BitSet, CombinationHelper> htable = new Hashtable<BitSet, CombinationHelper>();
										BitSet l2_bitset = pkg2.bitset;
										BitSet l1orl2 = (BitSet)l1_bitset.clone();
										l1orl2.or(l2_bitset);
										StrippedPartition pkg2_sp = l_small_cache.get(l2_bitset);
								     
										if (!pkg1.prefix_index.equals(pkg2.prefix_index) ||
										       l1_bitset.equals(l2_bitset) ||
										       (l1orl2.cardinality() - l1_bitset.cardinality() != 1) ||
										       (l1orl2.cardinality() - l2_bitset.cardinality() != 1)) {
										    if(!pkg1.prefix_index.equals(pkg2.prefix_index)) {
										      continue;
										   }
										   if(l1_bitset.equals(l2_bitset)){
										      continue;
										   }
										   if((l1orl2.cardinality() - l1_bitset.cardinality() != 1)) {
										      continue;
										   }
										   if((l1orl2.cardinality() - l2_bitset.cardinality() != 1) ) {
										      continue;
										   }
										}
										if (skipset.contains(l1orl2)) {
										       continue;
										}else{
										       skipset.add(l1orl2);
										}
										combination_counter++;
										boolean xIsValid = true;
										// clone of X for usage in the following loop
										BitSet Xclone = (BitSet) l1orl2.clone();
										// print_dependency2(Xclone, "original: ");
										for (int l = l1orl2.nextSetBit(0); l >= 0; l = l1orl2.nextSetBit(l + 1)) {
											Xclone.clear(l);
											if (!b_level0_keyset.value().contains(Xclone)) {
												// print_dependency2(Xclone, "check: false : ");
												xIsValid = false;
												break;
											}else{
												// print_dependency2(Xclone, "check: true : ");
											}
											Xclone.set(l);
										}
								
										if (!xIsValid) {
											continue;
										}
								
										after_checksubset++;
								
								
										StrippedPartition st = null;
										CombinationHelper combinationhelper = new CombinationHelper();
										if (pkg1.ch.isValid() && pkg2.ch.isValid()){
											Vector<Integer> tTable = new Vector<Integer>(b_numberTuples.value());
								
											for (long i = 0; i < b_numberTuples.value(); i++) tTable.add(-1);
								
											// =============== multiply =========================
											multiply_times++;
											List<List<Integer>> result = new ArrayList<List<Integer>>();
											List<List<Integer>> pt1List = (List)(((ArrayList)pkg1_sp.getStrippedPartition()).clone());
											List<List<Integer>> pt2List = (List)(((ArrayList)pkg2_sp.getStrippedPartition()).clone());
											System.out.println(" -- STATS: list-1 size: " + pt1List.size());
											System.out.println(" -- STATS: list-2 size: " + pt2List.size());
											System.out.println(" -- STATS: list-1 #ofElements: " + pkg1_sp.getElementCount());
											System.out.println(" -- STATS: list-2 #ofElements: " + pkg2_sp.getElementCount());
											long noOfElements = 0;
											
											for (Integer i = 0; i < pt1List.size(); i++) {
												int lst1_size = pt1List.get(i).size();
												List<Integer> lst1 = pt1List.get(i);
												System.out.println(" -- STATS: sub-list-1 min max: " + lst1.get(0)+ " "+ lst1.get(lst1_size-1));
												for (Integer j = 0; j < pt2List.size(); j++) {
													int lst2_size = pt2List.get(j).size();
													List<Integer> lst2 = pt2List.get(j);
													System.out.println(" -- STATS: sub-list-2 min max: " + lst2.get(0)+ " "+ lst2.get(lst2_size-1));
													int lst1_ptr = 0;
													int lst2_ptr = 0;
													List<Integer> lst_new = new ArrayList<Integer>();
													while(lst1_ptr < lst1_size && lst2_ptr < lst2_size) {
														if(lst1.get(lst1_ptr) == lst2.get(lst2_ptr)) {
															lst_new.add(lst1.get(lst1_ptr));
															lst1_ptr++;
															lst2_ptr++;
														}
														else if(lst1.get(lst1_ptr) < lst2.get(lst2_ptr))
															lst1_ptr++;
														else if(lst1.get(lst1_ptr) > lst2.get(lst2_ptr))
															lst2_ptr++;
													}
													if(lst_new.size() >= 2) {
														result.add(lst_new);
														noOfElements += lst_new.size();
													}
												}
											}
											st = new StrippedPartition(result, noOfElements);
											// =============== end of multiply =========================
										}else{
											combinationhelper.setInvalid();
										}
								
										BitSet rhsCandidates = new BitSet();
										
										//combinationhelper.setPartition(st);
										combinationhelper.setRhsCandidates(rhsCandidates);
										if(st != null) {
											combinationhelper.setElementCount(st.getElementCount());
											combinationhelper.setError(st.getError());
										}
								
										htable.put(l1orl2, combinationhelper);
										
										propertiesAccumulator.add(htable);
										// htable.clear();
										String combination_name = l1orl2.toString();
										combination_name = combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
								 
										try {
											FSDataOutputStream out = fs.create(new Path("hdfs://husky-06:8020/tmp/tane_nocache/" + b_cur_level.value()+"_"+combination_name));
											BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
											//Gson gson = new Gson();
											br.write(gson.toJson(st));
											br.close();
											out.close();
										} catch (IllegalArgumentException e) {
											e.printStackTrace();
										} catch (IOException e) {
											e.printStackTrace();
										}
										st = null;
									}
									pkg1_sp = null;
								}
                              
                         }
                  });

                  foreach_rdd += (System.nanoTime() - foreach_rdd_start);

                  Long mapTime = System.nanoTime();
                  Hashtable<BitSet, CombinationHelper> new_level = propertiesAccumulator.value();
                  my_print("Generate next level time " + 0.001* (System.nanoTime() - broadcast_package_rdd_start)/1000000 + "sec");
                  level1 = new_level;
           }

			private static List<BitSet[]> getListCombinations(List<BitSet> list) {
				List<BitSet[]> combinations = new ArrayList<BitSet[]>();
			    for (int a = 0; a < list.size(); a++) {
			    	for (int b = a + 1; b < list.size(); b++) {
			    		BitSet[] combi = new BitSet[2];
			            combi[0] = list.get(a);
			            combi[1] = list.get(b);
			            combinations.add(combi);
			    	}
			    }
			    return combinations;
			}

			
			// ========================= FUNCTIONS FOR SP ON DF APPROACH =================
			
			static Map<String, Integer> uniques = new HashMap<String, Integer>(); // maintain a list of uniques for pruning
			static Map<BitSet, Integer> toPrune = new HashMap<BitSet, Integer>();
			public static void execute2(int partition_number) {
				numberAttributes = numberAttributes-1;
				df.cache();
		        numberTuples = (new Long(df.count())).intValue();
		        System.out.println(" # of tuples: " + numberTuples);
		        
				// Create a set of ints, equale to the num of attributes
				Set<Integer> mySet = new HashSet<Integer>();
				for(int i = 0; i < numberAttributes; i++)
					mySet.add(i);
				
				// Levelwise map of powerset
				HashMap<Integer, ArrayList<Set<Integer>>> hm = new HashMap<Integer, ArrayList<Set<Integer>>>();
				for(int i = 0; i <= numberAttributes; i ++)
					hm.put(i, new ArrayList<Set<Integer>>());

				// populate the levelwise map.
				for (Set<Integer> s : powerSet(mySet)) {
					if(s != null)
						hm.get(s.size()).add(s);
		        }
				
				HashMap<String, Integer> metadata = new HashMap<String, Integer>();
				List<HashMap<String, HashSet<String>>> FD_list = new ArrayList<HashMap<String, HashSet<String>>>();
				for(int i = 1; i < numberAttributes; i++) {
					Long start = System.currentTimeMillis();
			        generateLevel(i, hm, metadata, FD_list, partition_number);
			        Long end = System.currentTimeMillis();
			        my_print("Level "+i+" " + (end - start)/1000 + " sec");
				}
				//for(Entry<String,Integer> e : metadata.entrySet())
				//	System.out.println(e.getKey()+" "+e.getValue());
				
			}
			
			private static <T> Set<Set<T>> powerSet(Set<T> originalSet) {
			    Set<Set<T>> sets = new HashSet<Set<T>>();
			    if (originalSet.isEmpty()) {
			    	sets.add(new HashSet<T>());
			    	return sets;
			    }
			    List<T> list = new ArrayList<T>(originalSet);
			    T head = list.get(0);
			    Set<T> rest = new HashSet<T>(list.subList(1, list.size())); 
			    for (Set<T> set : powerSet(rest)) {
			    	Set<T> newSet = new HashSet<T>();
			    	newSet.add(head);
			    	newSet.addAll(set);
			    	sets.add(newSet);
			    	sets.add(set);
			    }		
			    return sets;
			}
			
			private static void generateLevel(int level, 
					HashMap<Integer, ArrayList<Set<Integer>>> hm, 
					HashMap<String, Integer> metadata, 
					List<HashMap<String, HashSet<String>>> FD_list,
					int partition_number) {

				ArrayList<Set<Integer>> combination_list = hm.get(level);
				//System.out.println(combination_list.toString());
				int[][] combination_arr = new int[partition_number][level];
				int full = 0; // checks how many entries added to combination_arr
				for(int l = 0; l < combination_list.size(); l++) {
					// convert set to array
					int[] s_arr = new int[combination_list.get(l).size()];
					int m = 0;
					BitSet bs = new BitSet(numberAttributes);
					for(Integer elem : combination_list.get(l)) {
						s_arr[m++] = elem;
						bs.set(elem);
					}
					if(toPrune.containsKey(bs))
						continue;
					
					int i = 0;
					for(m = 0; m < s_arr.length; m++)
						combination_arr[full][i++] = s_arr[m];
					full++;
					
					if(full == partition_number || l == combination_list.size()-1) { // then process the batch
						Map<String, Integer> map = generateStrippedPartitions(combination_arr, full);
						Iterator<Entry<String, Integer>> entry_itr = map.entrySet().iterator();
						while(entry_itr.hasNext()){
							Entry<String, Integer> e = entry_itr.next();
							metadata.put(e.getKey(), e.getValue());
							if(e.getValue() == numberTuples)
								uniques.put(e.getKey(), e.getValue());
						}
						full = 0;
						combination_arr = new int[partition_number][level];
					}
				}
				
				toPrune.clear();
				
				// check for FDs
				for(int l = 0; l < combination_list.size(); l++) {
					// convert set to array
					int[] s_arr = new int[combination_list.get(l).size()];
					int m = 0;
					for(Integer elem : combination_list.get(l))
						s_arr[m++] = elem;
					
					// check is FD or not
					int skip = 0;
					for(int j = 0; j < s_arr.length; j++) {
						String lhs_key = "";
						String rhs_key = "";
						String fd_rhs = "";
						HashSet<String> fd_lhs = new HashSet<String>();
						for(int k = 0; k < s_arr.length; k++) {
							rhs_key += "_"+s_arr[k];
							if(k != skip){
								lhs_key += "_"+s_arr[k];
								fd_lhs.add(s_arr[k]+"");
							}
							else
								fd_rhs += s_arr[k];
						}
						//System.out.println(" checking" + lhs_key + " "+ rhs_key);
						if(metadata.get(lhs_key) == metadata.get(rhs_key)) {
							HashMap<String, HashSet<String>> tmp_map = new HashMap<String, HashSet<String>>();
							tmp_map.put(fd_rhs, fd_lhs);
							FD_list.add(tmp_map);
							//String FD = fd_lhs + "->" + fd_rhs+";";
							//System.out.println("FD "+FD);
						}
						skip++;
					}
				} //. end check for FDs
				
				// generate pruned combinations for next level
				Iterator<Entry<String, Integer>> itr1 = uniques.entrySet().iterator();
				
				while(itr1.hasNext()){
					Iterator<Entry<String, Integer>> itr2 = uniques.entrySet().iterator();
					Entry<String, Integer> e1 = itr1.next();
					BitSet bs1 = stringToBitset(e1.getKey());
					while(itr2.hasNext()) {
						Entry<String, Integer> e2 = itr2.next();
						if(e1.getKey().equals(e2.getKey()))
							continue;
						BitSet bs2 = stringToBitset(e2.getKey());
						bs1.or(bs2);
						toPrune.put(bs1, 1);
					}
				}
				uniques.clear();
			}
			
			public static Map<String, Integer> generateStrippedPartitions(int[][] combinations, int num_combinations) {
		    	final Broadcast<int[][]> b_combinations = sc.broadcast(combinations);
		    	final Broadcast<Integer> b_num_combinations = sc.broadcast(num_combinations);
		    	JavaRDD<Row> dataRDD = df.javaRDD();
		    	
		    	// Row = <C0, C1, C2, ... , index>
		    	
		    	JavaRDD<Tuple2<String,Long>> tupleRDD = dataRDD.repartition(14).flatMap(new FlatMapFunction<Row, Tuple2<String,Long>>() {
		    		public Iterator<Tuple2<String,Long>> call(Row r) {
		    			List<Tuple2<String,Long>> l = new LinkedList<Tuple2<String,Long>>();
		    			for(int i = 0; i < b_num_combinations.value(); i++) {
		    				String value = "";
		    				String colComb = "";
							for(int j = 0; j < b_combinations.value()[i].length; j++) {
								value += "_"+r.getLong(b_combinations.value()[i][j]);
								colComb += "_"+b_combinations.value()[i][j];
							}
							String key = colComb + "-" + value;
							l.add(new Tuple2<String, Long>(key, r.getLong(r.size()-1)));
						}
		    			return l.iterator();
		    		}
		    	});
		    	
		    	JavaPairRDD<String, Long> mapRDD = tupleRDD.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
		    		public Tuple2<String, Long> call(Tuple2<String, Long> t) {
		    			return new Tuple2<String, Long>(t._1, t._2);
		    		}
		    	});
		    	
		    	JavaPairRDD<String, Integer> spRDD = mapRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Long>>, String, Integer>() {
		    		public Tuple2<String, Integer> call(Tuple2<String, Iterable<Long>> t) {
		    			String[] splits = t._1.split("-");
		    			return new Tuple2(splits[0], 1);
		    		}
		    	});
		    	
		    	JavaPairRDD<String, Integer> attrSpRDD = spRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
		    		public Integer call(Integer i, Integer j){
		    			return i+j;
		    		}
		    	});
		    	
		    	//for(Tuple2<String, Integer> t : attrSpRDD.collect())
		    	//	System.out.println("-- " + t._1+" = "+t._2);
		    	
		    	return attrSpRDD.collectAsMap();
		    	
			}

			public static BitSet stringToBitset(String str) {
				BitSet bs = new BitSet(numberAttributes);
				String[] splitArr = str.split("_");
				for(int i = 0; i < splitArr.length; i++) {
					if(splitArr[i] != ""){
						int pos = Integer.parseInt(splitArr[i]);
						bs.set(pos);
					}
				}
				return bs;
			}
			
			/*public static String bitsetToString(BitSet bs) {
			}*/
			
			// ===================== END OF FUNCTIONS ================================

              private static void buildPrefixBlocks() {
                 prefix_blocks.clear();
                 for (BitSet level_iter : level0.keySet()) {
                    BitSet prefix = getPrefix(level_iter);

                    if (prefix_blocks.containsKey(prefix)) {
                       prefix_blocks.get(prefix).add(level_iter);
                    } else {
                       List<BitSet> list = new ArrayList<BitSet>();
                       list.add(level_iter);
                       prefix_blocks.put(prefix, list);
                    }
                 }
              }

              private static int getLastSetBitIndex(BitSet bitset) {
                 int lastSetBit = 0;
                 for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
                    lastSetBit = A;
                 }
                 return lastSetBit;
              }

              private static BitSet getPrefix(BitSet bitset) {
                 BitSet prefix = (BitSet) bitset.clone();
                 prefix.clear(getLastSetBitIndex(prefix));
                 return prefix;
              }

              private static boolean checkSubsets(BitSet X) {
                 boolean xIsValid = true;
                 // clone of X for usage in the following loop
                 BitSet Xclone = (BitSet) X.clone();
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

			public static void my_print_args(String[] args){
				 for (int i = 0; i < args.length; i++) {
					 my_print("args[" + i + "]:\t" + args[i]);
				 }
			}

  public static void insert(BitSet X, Integer A){

  }

  public static void print_dependency2(BitSet set, String prefix){
                     StringBuffer stringBuffer = new StringBuffer(prefix);
                     for (int l = set.nextSetBit(0); l >= 0; l = set.nextSetBit(l + 1)) {
                            stringBuffer.append(columnNames[l-1] + ", ");
                     }
                     my_print(stringBuffer.toString());
  }

  public static void print_dependency(BitSet set, Integer A, String[] columnNames){
                     StringBuffer stringBuffer = new StringBuffer("=============================================");
                     for (int l = set.nextSetBit(0); l >= 0; l = set.nextSetBit(l + 1)) {
                            stringBuffer.append(columnNames[l-1] + ", ");
                     }
                     stringBuffer.append(" => " + columnNames[A-1]);
                     my_print(stringBuffer.toString());
  }

  public static void print_dependency(BitSet set, Integer A){
	  				fdCount++;
                     /*StringBuffer stringBuffer = new StringBuffer("=============================================");
                     for (int l = set.nextSetBit(0); l >= 0; l = set.nextSetBit(l + 1)) {
                            stringBuffer.append(columnNames[l-1] + ", ");
                     }
                     stringBuffer.append(" => " + columnNames[A-1]);
                     my_print(stringBuffer.toString());*/
  }


  public static void my_print(Boolean val){
                     if(val) System.out.println("+++++ TRUE");
                     else System.out.println("+++++ FALSE");
  }

  public static void my_print(String val){
                     System.out.println("## " + val);
  }
  public static void my_print(Integer val){
                     System.out.println("## " + val);
  }
  public static void my_print(long val){
                     System.out.println("## " + val);
  }
}
