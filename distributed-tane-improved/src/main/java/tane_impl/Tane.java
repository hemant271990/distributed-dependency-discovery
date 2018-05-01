package tane_impl;

import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import tane_helper.*;
import tane_impl.*;

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

public class Tane {

    public static int numberAttributes;
    public static long numberTuples;
    public static String[] columnNames;
    //public static ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    public static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level0 = null;
    public static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level1 = null;
    public static Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>> prefix_blocks = null;
    public static LongBigArrayBigList tTable;
    public static Dataset<Row> df = null;
	public static JavaSparkContext sc;
	public static String datasetFile = null;
	public static int partition_number = 0;
	public static int curr_level = 0;
	public static int fdCount = 0;
	
    public static void execute() {

        level0 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        level1 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        prefix_blocks = new Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>>();

        // Get information about table from database or csv file
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = loadData();
        //setColumnIdentifiers();

        System.out.println(" # Attributes: " + numberAttributes);
        System.out.println(" # Tuples: " + numberTuples);
        
        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        // Initialize Level 0
        CombinationHelper chLevel0 = new CombinationHelper();
        OpenBitSet rhsCandidatesLevel0 = new OpenBitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
        chLevel0.setPartition(spLevel0);
        spLevel0 = null;
        level0.put(new OpenBitSet(), chLevel0);
        chLevel0 = null;

        // Initialize Level 1
               
        for (int i = 1; i <= numberAttributes; i++) {
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
        partitions = null;

        // while loop (main part of TANE)
        int l = 1;
        while (!level1.isEmpty() && l <= numberAttributes) {
			//getSizeofLevel(level1);	
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
		System.out.println("FD Count: "+fdCount);
    }

    public static OpenBitSet stringToBitset(String str) {
		OpenBitSet bs = new OpenBitSet();
		String[] splitArr = str.split("_");
		for(int i = 0; i < splitArr.length; i++) {
			if(splitArr[i] != ""){
				int pos = Integer.parseInt(splitArr[i]);
				bs.set(pos);
			}
		}
		return bs;
	}
    
    /**
     * Loads the data from the database or a csv file and
     * creates for each attribute a HashMap, which maps the values to a List of tuple ids.
     *
     * @return A ObjectArrayList with the HashMaps.
     * @throws InputGenerationException
     * @throws InputIterationException
     */
    private static ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> loadData() {
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(numberAttributes);
        for (int i = 0; i < numberAttributes; i++) {
            Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<Object, LongBigArrayBigList>();
            partitions.add(partition);
        }
        long tupleId = 0;
        List<Row> tuple_list = df.collectAsList();
        for(int t = 0; t < tuple_list.size(); t++) {
            Row row = tuple_list.get(t);
            for (int i = 0; i < numberAttributes; i++) {
                Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = partitions.get(i);
                Long entry = row.getLong(i);
                String entry_str = entry.toString();
                if (partition.containsKey(entry_str)) {
                    partition.get(entry_str).add(tupleId);
                } else {
                    LongBigArrayBigList newEqClass = new LongBigArrayBigList();
                    newEqClass.add(tupleId);
                    partition.put(entry_str, newEqClass);
                }
                ;
            }
            tupleId++;
        }
        numberTuples = tupleId;
        return partitions;
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
                    StrippedPartition spXwithoutA = level0.get(Xclone).getPartition();
                    StrippedPartition spX = level1.get(X).getPartition();
                    
                    if (spX.getError() == spXwithoutA.getError()) {
                    	
                        // found Dependency
                        OpenBitSet XwithoutA = (OpenBitSet) Xclone.clone();
                        processFunctionalDependency(XwithoutA, A);

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
            if (level1.get(x).isValid() && level1.get(x).getPartition().getError() == 0) {

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

    /**
     * Calculate the product of two stripped partitions and return the result as a new stripped partition.
     *
     * @param pt1: First StrippedPartition
     * @param pt2: Second StrippedPartition
     * @return A new StrippedPartition as the product of the two given StrippedPartitions.
     */
    public static StrippedPartition multiply(StrippedPartition pt1, StrippedPartition pt2) {
        ObjectBigArrayBigList<LongBigArrayBigList> result = new ObjectBigArrayBigList<LongBigArrayBigList>();
        ObjectBigArrayBigList<LongBigArrayBigList> pt1List = pt1.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> pt2List = pt2.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> partition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        long noOfElements = 0;
        // iterate over first stripped partition and fill tTable.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, i);
            }
            partition.add(new LongBigArrayBigList());
        }
        // iterate over second stripped partition.
        for (long i = 0; i < pt2List.size64(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable.get(t_id) != -1) {
                    partition.get(tTable.get(t_id)).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable.get(tId) != -1) {
                    if (partition.get(tTable.get(tId)).size64() > 1) {
                        LongBigArrayBigList eqClass = partition.get(tTable.get(tId));
                        result.add(eqClass);
                        noOfElements += eqClass.size64();
                    }
                    partition.set(tTable.get(tId), new LongBigArrayBigList());
                }
            }
        }
        // cleanup tTable to reuse it in the next multiplication.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, -1);
            }
        }
        return new StrippedPartition(result, noOfElements);
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
                    StrippedPartition st = null;
                    CombinationHelper ch = new CombinationHelper();
                    if (level0.get(c[0]).isValid() && level0.get(c[1]).isValid()) {
                    	pruned_list.add(X);
                        st = multiply(level0.get(c[0]).getPartition(), level0.get(c[1]).getPartition());
                    } else {
                        ch.setInvalid();
                    }
                    OpenBitSet rhsCandidates = new OpenBitSet();

                    ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);

                    new_level.put(X, ch);
                }
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
    			System.out.print(i-1);
        System.out.print(" -> " + (a-1)+"\n");
    }

    /*private void setColumnIdentifiers() {
        this.columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(this.columnNames.size());
        for (String column_name : this.columnNames) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, column_name));
        }
    }*/

	public static long getSizeofLevel(Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level) {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		long size = 0;
		//long size = 0;
		for(Entry<OpenBitSet, CombinationHelper> en : level.entrySet()){
			try {
			  out = new ObjectOutputStream(bos);
				if(en.getValue().getPartition() == null)
				  continue;
			  out.writeObject(en.getValue().getPartition().getStrippedPartition());
			  out.flush();
			  byte[] yourBytes = bos.toByteArray();
			  size += yourBytes.length;
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
			  bos.close();
				} catch (IOException ex) {
			    // ignore close exception
			  }
			}
		}
		System.out.println("Java serialization level0 length in bytes " + size);
		return size;
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
}
