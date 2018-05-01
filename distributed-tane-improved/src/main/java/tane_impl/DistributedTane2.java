package tane_impl;
/*
 * Broadcasts data once and use it in every iteration. 
 */
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import tane_helper.*;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DistributedTane2 {

    public static String tableName;
    public static int numberAttributes;
    public static Long numberTuples;
    public static String[] columnNames;
    public static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level0 = null;
    public static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level1 = null;
    public static Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>> prefix_blocks = null;
    public static LongBigArrayBigList tTable;
    public static Dataset<Row> df = null;
	public static JavaSparkContext sc;
	public static String datasetFile = null;
	public static int batch_size = 0;
	public static int curr_level = 0;
	public static int fdCount = 0;
	public static int spComputation = 0;
	public static int numSparkTasksFactor = 1;
	public static int numPartitions = 55;
	public static long latticeGenTime = 0;
	public static long genEQClassTime = 0;
	public static long pruneTime = 0;
	public static long computeDependencyTime = 0;
	
    public static void execute() {

        level0 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        level1 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        prefix_blocks = new Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>>();

        // Get information about table from database or csv file
        
        System.out.println(" # Attributes: " + numberAttributes);
        System.out.println(" # Tuples: " + numberTuples);
        
        // Initialize Level 0
        CombinationHelper chLevel0 = new CombinationHelper();
        OpenBitSet rhsCandidatesLevel0 = new OpenBitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        chLevel0.setUniqueCount(1);
        level0.put(new OpenBitSet(), chLevel0);
        chLevel0 = null;

        // Initialize Level 1
        int[][] combination_arr = new int[batch_size][1];
        int full = 0;
		for(int l = 1; l <= numberAttributes; l++) {
			// convert set to array
			int[] s_arr = new int[1];
			int m = 0;
			s_arr[0] = l;
			
			int i = 0;
			for(m = 0; m < s_arr.length; m++)
				combination_arr[full][i++] = s_arr[m];
			full++;
			
			if(full == batch_size || l == numberAttributes) { // then process the batch
				Map<String, Integer> map = generateStrippedPartitions(combination_arr, full);
				Iterator<Entry<String, Integer>> entry_itr = map.entrySet().iterator();
				while(entry_itr.hasNext()){
					Entry<String, Integer> e = entry_itr.next();
					OpenBitSet combinationLevel1 = stringToBitset(e.getKey());
					
					CombinationHelper chLevel1 = new CombinationHelper();
		            OpenBitSet rhsCandidatesLevel1 = new OpenBitSet();
		            rhsCandidatesLevel1.set(1, numberAttributes + 1);
		            chLevel1.setRhsCandidates(rhsCandidatesLevel0);
		            chLevel1.setUniqueCount(e.getValue());;
		            level1.put(combinationLevel1, chLevel1);
				}
				full = 0;
				combination_arr = new int[batch_size][1];
			}
		}
        
		int[][] table = new int[numberTuples.intValue()][numberAttributes];
    	int currRow = 0;
    	List<Row> rowList = df.collectAsList();
    	for(Row r : rowList) {
    		for(int i = 0; i < numberAttributes; i++) {
    			table[currRow][i] = new Long(r.getLong(i)).intValue();
    		}
    		currRow++;
    	}
    	final Broadcast<int[][]> b_table = sc.broadcast(table);

    	// DEBUG
    	/*for(int i = 0; i < b_table.value().length; i++){
    		for(int j = 0; j < table[i].length; j++)
    			System.out.print(" "+table[i][j]);
    		System.out.println();
    	}*/
    	
        // while loop (main part of TANE)
        int l = 1;
        while (!level1.isEmpty() && l <= numberAttributes) {
        	curr_level = l;
            // compute dependencies for a level
			long t1 = System.currentTimeMillis();
            computeDependencies();
            long t2 = System.currentTimeMillis();
            computeDependencyTime += t2-t1;
            
            // prune the search space
            prune();
            
            long t3 = System.currentTimeMillis();
            pruneTime += t3-t2;
            
            // compute the combinations for the next level
            generateNextLevel(b_table);
            long t4 = System.currentTimeMillis();
            l++;
            System.out.println(" Level: "+l + " time(s): " + (t4-t1)/1000);
        }
        System.out.println("latticeGenTime: "+latticeGenTime/1000);
        System.out.println("genEQClassTime: "+genEQClassTime/1000);
        System.out.println("computeDependencyTime: "+computeDependencyTime/1000);
        System.out.println("pruneTime: "+pruneTime/1000);
		System.out.println("FD Count: " + fdCount);
		System.out.println(" # of SP computations: " + spComputation);
    }

    public static OpenBitSet stringToBitset(String str) {
		OpenBitSet bs = new OpenBitSet();
		String[] splitArr = str.split("_");
		for(int i = 0; i < splitArr.length; i++) {
			if(!splitArr[i].equals("")){
				int pos = Integer.parseInt(splitArr[i]);
				bs.set(pos);
			}
		}
		return bs;
	}

    /**
     * Initialize Cplus (resp. rhsCandidates) for each combination of the level.
     */
    private static void initializeCplusForLevel() {
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
            ch.setRhsCandidates(CforX);
        }
    }

    /**
     * Computes the dependencies for the current level (level1).
     *
     * @throws AlgorithmExecutionException
     */
    private static void computeDependencies() {
        initializeCplusForLevel();

        // iterate through the combinations of the level
        for (OpenBitSet X : level1.keySet()) {
            if (level1.get(X).isValid()) {
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
                    long spXwithoutACount = level0.get(Xclone).getUniqueCount();
                    long spXCount = level1.get(X).getUniqueCount();

                    if (spXCount == spXwithoutACount) {
                        // found Dependency
                        OpenBitSet XwithoutA = (OpenBitSet) Xclone.clone();
                        processFunctionalDependency(XwithoutA, A); // uncomment this to print FDs

                        // remove A from C_plus(X)
                        level1.get(X).getRhsCandidates().clear(A);

                        // remove all B in R\X from C_plus(X)
                        OpenBitSet RwithoutX = new OpenBitSet();
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

    /**
     * Prune the current level (level1) by removing all elements with no rhs candidates.
     * All keys are marked as invalid.
     * In case a key is found, minimal dependencies are added to the result receiver.
     *
     * @throws AlgorithmExecutionException if the result receiver cannot handle the functional dependency.
     */
    private static void prune() {
        ObjectArrayList<OpenBitSet> elementsToRemove = new ObjectArrayList<OpenBitSet>();
        for (OpenBitSet x : level1.keySet()) {
            if (level1.get(x).getRhsCandidates().isEmpty()) {
                elementsToRemove.add(x);
                continue;
            }
            // Check if x is a key. Thats the case, if the error is 0.
            // See definition of the error on page 104 of the TANE-99 paper.
            if (level1.get(x).isValid() && level1.get(x).getUniqueCount() == numberTuples) {

                // C+(X)\X
                OpenBitSet rhsXwithoutX = (OpenBitSet) level1.get(x).getRhsCandidates().clone();
                rhsXwithoutX.andNot(x);
                for (int a = rhsXwithoutX.nextSetBit(0); a >= 0; a = rhsXwithoutX.nextSetBit(a + 1)) {
                    OpenBitSet intersect = new OpenBitSet();
                    intersect.set(1, numberAttributes + 1);

                    OpenBitSet xUnionAWithoutB = (OpenBitSet) x.clone();
                    xUnionAWithoutB.set(a);
                    for (int b = x.nextSetBit(0); b >= 0; b = x.nextSetBit(b + 1)) {
                        xUnionAWithoutB.clear(b);
                        CombinationHelper ch = level1.get(xUnionAWithoutB);
                        if (ch != null) {
                            intersect.and(ch.getRhsCandidates());
                        } else {
                            intersect = new OpenBitSet();
                            break;
                        }
                        xUnionAWithoutB.set(b);
                    }

                    if (intersect.get(a)) {
                        OpenBitSet lhs = (OpenBitSet) x.clone();
                        processFunctionalDependency(lhs, a);
                        level1.get(x).getRhsCandidates().clear(a);
                        level1.get(x).setInvalid();
                    }
                }
            }
        }
        for (OpenBitSet x : elementsToRemove) {
            level1.remove(x);
        }
    }

    /**
     * Adds the FD lhs -> a to the resultReceiver and also prints the dependency.
     *
     * @param lhs: left-hand-side of the functional dependency
     * @param a:   dependent attribute. Possible values: 1 <= a <= maxAttributeNumber.
     * @throws CouldNotReceiveResultException if the result receiver cannot handle the functional dependency.
     */
    private static void processFunctionalDependency(OpenBitSet lhs, int a) {
		fdCount++;
        //addDependencyToResultReceiver(lhs, a);
    }

    private static long getLastSetBitIndex(OpenBitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
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

    private static void generateNextLevel(Broadcast<int[][]> b_table) {
        level0 = level1;
        level1 = null;

        long t1 = System.currentTimeMillis();
        Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> new_level = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();

        buildPrefixBlocks();
        LinkedList<OpenBitSet> pruned_list = new LinkedList<OpenBitSet>();
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
                    //StrippedPartition st = null;
                    CombinationHelper ch = new CombinationHelper();
                    if (level0.get(c[0]).isValid() && level0.get(c[1]).isValid()) {
                    	pruned_list.add(X);
                        //st = multiply(level0.get(c[0]).getPartition(), level0.get(c[1]).getPartition());
                    } else {
                        ch.setInvalid();
                    }
                    OpenBitSet rhsCandidates = new OpenBitSet();

                    //ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);

                    new_level.put(X, ch);
                }
            }
        }
        long t2 = System.currentTimeMillis();
        latticeGenTime += t2-t1;
        
        spComputation = spComputation + pruned_list.size();
        LinkedList<OpenBitSet> batch = new LinkedList<OpenBitSet>();
        int full = 0; // checks how many entries added to combination_arr
        for(int l = 0; l < pruned_list.size(); l++) {
			OpenBitSet bs = pruned_list.get(l);
			batch.add(bs);
			full++;
	        
	        if(full == batch_size || l == pruned_list.size()-1) { // then process the batch
	        	System.out.println("Running Spark job for batch size: "+batch.size());
		        JavaRDD<OpenBitSet> combinationsRDD = sc.parallelize(batch, numPartitions);
		        Map<String, Integer> map = generateStrippedPartitions(combinationsRDD, b_table);
				Iterator<Entry<String, Integer>> entry_itr = map.entrySet().iterator();
				while(entry_itr.hasNext()){
					Entry<String, Integer> e = entry_itr.next();
					//System.out.println(e.getKey()+" "+e.getValue());
					OpenBitSet combinationLevel1 = stringToBitset(e.getKey());
					new_level.get(combinationLevel1).setUniqueCount(e.getValue());
				}
				full = 0;
				batch = new LinkedList<OpenBitSet>();
	        }
        }
        long t3 = System.currentTimeMillis();
        genEQClassTime += t3-t2;
        level1 = new_level;
    }

    /**
     * Add the functional dependency to the ResultReceiver.
     *
     * @param X: A OpenBitSet representing the Columns of the determinant.
     * @param a: The number of the dependent column (starting from 1).
     * @throws CouldNotReceiveResultException if the result receiver cannot handle the functional dependency.
     */
    private static void addDependencyToResultReceiver(OpenBitSet X, int a) {
    	System.out.println();
    	for(int i = 1; i <= numberAttributes; i++)
    		if(X.get(i))
    			System.out.print(i+" ");
        System.out.print(" -> " + (a)+"\n");
    }

    public void serialize_attribute(OpenBitSet bitset, CombinationHelper ch) {
        String file_name = bitset.toString();
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(file_name));
            oos.writeObject(ch);
            oos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CombinationHelper deserialize_attribute(OpenBitSet bitset) {
        String file_name = bitset.toString();
        ObjectInputStream is = null;
        CombinationHelper ch = null;
        try {
            is = new ObjectInputStream(new FileInputStream(file_name));
            ch = (CombinationHelper) is.readObject();
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return ch;
    }
    
    public static Map<String, Integer> generateStrippedPartitions(JavaRDD<OpenBitSet> combinationsRDD, Broadcast<int[][]> b_table) {
    	final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
    	JavaPairRDD<String, Integer> attrSpRDD2 = combinationsRDD.mapToPair(new PairFunction<OpenBitSet, String, Integer>(){
    		public Tuple2<String, Integer> call(OpenBitSet b) {
    			HashSet<ArrayList<Integer>> hashSet = new HashSet<ArrayList<Integer>>();
    			String combination = "";
    			int[][] table = b_table.value();
    			for(int i = 1; i <= b_numberAttributes.value(); i++) {
    				if(b.get(i))
    					combination += "_"+i;
    			}
    			for(int i = 0; i < table.length; i++){
    				int[] row = table[i];
    				ArrayList<Integer> value = new ArrayList<Integer>();
    				for(int j = 1; j <= b_numberAttributes.value(); j++) {
        				if(b.get(j))
        					value.add(row[j-1]);
        			}
    				hashSet.add(value);
    			}
    			return new Tuple2<String, Integer>(combination, hashSet.size());
    		}
    	});
    	return attrSpRDD2.collectAsMap();
	}
    
 // Works best, least size
    public static Map<String, Integer> generateStrippedPartitions(int[][] combinations, int num_combinations) {
    	final Broadcast<int[][]> b_combinations = sc.broadcast(combinations);
    	final Broadcast<Integer> b_num_combinations = sc.broadcast(num_combinations);
    	JavaRDD<Row> dataRDD = df.javaRDD();
    	
    	// Row = <C0, C1, C2, ... , index>
    	
    	JavaRDD<ArrayList<Integer>> combValueRDD = dataRDD.repartition(numPartitions*10).flatMap(new FlatMapFunction<Row, ArrayList<Integer>>() {
    		public Iterator<ArrayList<Integer>> call(Row r) {
    			HashSet<ArrayList<Integer>> hash = new HashSet<ArrayList<Integer>>();
    			for(int i = 0; i < b_num_combinations.value(); i++) {
    				ArrayList<Integer> l = new ArrayList<Integer>();
    				l.add(i);
					for(int j = 0; j < b_combinations.value()[i].length; j++) {
						l.add((new Long(r.getLong(b_combinations.value()[i][j]-1))).intValue());
					}
					l.trimToSize();
					hash.add(l);
				}
    			return hash.iterator();
    		}
    	});
    	
    	JavaRDD<ArrayList<Integer>> distinctCombValueRDD = combValueRDD.distinct();
    	
    	JavaPairRDD<Integer, Integer> spRDD = distinctCombValueRDD.mapToPair(new PairFunction<ArrayList<Integer>, Integer, Integer>() {
    		public Tuple2<Integer, Integer> call(ArrayList<Integer> s) {
    			return new Tuple2<Integer, Integer>(s.get(0), 1);
    		}
    	});
    	
    	JavaPairRDD<Integer, Integer> attrSpRDD = spRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
    		public Integer call(Integer i, Integer j){
    			return i+j;
    		}
    	});
    	
    	JavaPairRDD<String, Integer> attrSpRDD2 = attrSpRDD.mapToPair(new PairFunction<Tuple2<Integer,Integer>, String, Integer>() {
    		public Tuple2<String, Integer> call(Tuple2<Integer,Integer> t) {
    			int idx = t._1;
    			int[] combArr = b_combinations.value()[idx];
    			String combStr = "";
    			for(int i = 0; i < combArr.length; i++)
    				combStr += "_"+combArr[i];
    			return new Tuple2<String, Integer>(combStr, t._2);
    		}
    	});
    	//for(Tuple2<String, Integer> t : attrSpRDD.collect())
    	//	System.out.println("-- " + t._1+" = "+t._2);
    	
    	return attrSpRDD2.collectAsMap();
	}
}
