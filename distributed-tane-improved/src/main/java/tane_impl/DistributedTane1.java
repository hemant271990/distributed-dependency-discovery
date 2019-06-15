package tane_impl;
/*
 * This is a smPDP small memory plan
 */
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import tane_helper.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.google.gson.Gson;

public class DistributedTane1 {

    public static String tableName;
    public static int numberAttributes;
    public static long numberTuples;
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
				Map<String, Integer> map = generateStrippedPartitionsGroupBy(combination_arr, full);
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
        
        /*for (int i = 1; i <= numberAttributes; i++) {
            OpenBitSet combinationLevel1 = new OpenBitSet();
            combinationLevel1.set(i);

            CombinationHelper chLevel1 = new CombinationHelper();
            OpenBitSet rhsCandidatesLevel1 = new OpenBitSet();
            rhsCandidatesLevel1.set(1, numberAttributes + 1);
            chLevel1.setRhsCandidates(rhsCandidatesLevel0);

            StrippedPartition spLevel1 = new StrippedPartition(partitions.get(i - 1));
            chLevel1.setPartition(spLevel1);

            level1.put(combinationLevel1, chLevel1);
        }
        partitions = null;*/

        // while loop (main part of TANE)
        int l = 1;
        while (!level1.isEmpty() && l <= numberAttributes) {
        	curr_level = l;
			Long t1 = System.currentTimeMillis();
            // compute dependencies for a level
            computeDependencies();

            // prune the search space
            prune();

            // compute the combinations for the next level
            generateNextLevel();
            l++;
			Long t2 = System.currentTimeMillis();
            System.out.println(" Level: "+l + " time(s): " + (t2-t1)/1000);
        }
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

    private static void generateNextLevel() {
        level0 = level1;
        level1 = null;
        //System.gc();

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

        spComputation = spComputation + pruned_list.size();
        int[][] combination_arr = new int[batch_size][curr_level+1];
		int full = 0; // checks how many entries added to combination_arr
		for(int l = 0; l < pruned_list.size(); l++) {
			OpenBitSet bs = pruned_list.get(l);
			int[] s_arr = new int[curr_level+1];
			int m = 0;
			for(int i = 1; i <= numberAttributes; i++) {
				if(bs.get(i))
					s_arr[m++] = i;
			}
			int i = 0;
			for(m = 0; m < s_arr.length; m++)
				combination_arr[full][i++] = s_arr[m];
			full++;
			
			if(full == batch_size || l == pruned_list.size()-1) { // then process the batch
				System.out.println(" Starting Spark job at level: "+curr_level + " for batch size: "+full);
				Map<String, Integer> map = generateStrippedPartitionsGroupBy(combination_arr, full);
				Iterator<Entry<String, Integer>> entry_itr = map.entrySet().iterator();
				while(entry_itr.hasNext()){
					Entry<String, Integer> e = entry_itr.next();
					OpenBitSet combinationLevel1 = stringToBitset(e.getKey());
					new_level.get(combinationLevel1).setUniqueCount(e.getValue());
				}
				full = 0;
				combination_arr = new int[batch_size][curr_level+1];
			}
		}
		
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
    
    public static Map<String, Integer> generateStrippedPartitionsGroupBy(int[][] combinations, int num_combinations) {
    	Map<String, Integer> result = new HashMap<String, Integer>();
    	for(int i = 0; i < num_combinations; i++){
    		List<String> comb = new ArrayList<String>();
    		String combStr = "";
    		for(int j = 0; j < combinations[i].length; j++) {
    			comb.add(df.columns()[combinations[i][j]-1]);
    			combStr += "_"+combinations[i][j];
    			//System.out.println(i+" "+j+" "+combinations[i][j]+" "+ df.columns()[combinations[i][j]-1]);
    		}
    		String firstC = comb.remove(0);
    		Seq<String> combSeq = JavaConverters.asScalaIteratorConverter(comb.iterator()).asScala().toSeq();
    		Long count = df.agg(functions.countDistinct(firstC, combSeq)).collectAsList().get(0).getLong(0);
    		//System.out.println(" Count for "+firstC+" "+count);
    		result.put(combStr, count.intValue());
    	}
    	return result;
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
