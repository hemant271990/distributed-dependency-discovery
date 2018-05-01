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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
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
public class DistributedFastOD2 {

    private static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level_minus1 = null;
    private static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level0 = null;
    private static Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level1 = null;
    private static Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>> prefix_blocks = null;

    private static String tableName;
    public static int numberAttributes;
    public static Long numberTuples;
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

    static List<FDODScore2> FDODScoreList;
    static Map<OpenBitSet, Long> XScoreMap = new HashMap<OpenBitSet, Long>();

    int answerCountFD = 1;
    int answerCountOD = 1;
    private static int numberOfOD = 0;
    private static int numberOfFD = 0;
    private static int curr_level = 0;
    public static int sideLen = 10;
    public static int numPartitions = 55;
    
    public static void execute() {

        FDODScoreList = new ArrayList<FDODScore2>();

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
        //StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
        ObjectArrayList<OpenBitSet> swapCandidatesLevel0 = new ObjectArrayList<OpenBitSet>();//the C_s is empty for L0
        chLevel0.setSwapCandidates(swapCandidatesLevel0);
        //chLevel0.setElementCount(spLevel0.getElementCount());
        //chLevel0.setError(spLevel0.getError());
        chLevel0.setUniqueCount(1);
        //spLevel0 = null;
        level0.put(new OpenBitSet(), chLevel0);
        chLevel0 = null;
        //End Initialize Level 0

        //OD
        TAU_SortedList = new ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>>();
        attributeValuesList = new ArrayList<ObjectBigArrayBigList<Integer>>();

        //Begin Initialize Level 1
        int[][] combination_arr = new int[batch_size][1];
        int full = 0;
        for (int l = 1; l <= numberAttributes; l++) {
        	StrippedPartition spLevel1 = new StrippedPartition(partitions.get(l - 1), TAU_SortedList, attributeValuesList, numberTuples);
        	// convert set to array
			int[] s_arr = new int[1];
			int m = 0;
			s_arr[0] = l;
			
			int j = 0;
			for(m = 0; m < s_arr.length; m++)
				combination_arr[full][j++] = s_arr[m];
			full++;
        	
			if(full == batch_size || l == numberAttributes) { // then process the batch
				Map<String, Integer> map = generateStrippedPartitions(combination_arr, full);
				Iterator<Entry<String, Integer>> entry_itr = map.entrySet().iterator();
				while(entry_itr.hasNext()){
					Entry<String, Integer> e = entry_itr.next();
					//System.out.println(e.getKey());
					OpenBitSet combinationLevel1 = stringToBitset(e.getKey());
					
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
		            
		            chLevel1.setUniqueCount(e.getValue());
		            level1.put(combinationLevel1, chLevel1);
				}
				full = 0;
				combination_arr = new int[batch_size][1];
			}
			
            //CombinationHelper chLevel1 = new CombinationHelper();
            //OpenBitSet rhsCandidatesLevel1 = new OpenBitSet();
            //rhsCandidatesLevel1.set(1, numberAttributes + 1);
            //chLevel1.setRhsCandidates(rhsCandidatesLevel0);

            //ObjectArrayList<OpenBitSet> swapCandidatesLevel1 = new ObjectArrayList<OpenBitSet>();//the C_s is empty for L1
            //chLevel1.setSwapCandidates(swapCandidatesLevel1);

            //we also initialize TAU_SortedList with all equivalent classes, even for size 1
            //StrippedPartition spLevel1 =
            //        new StrippedPartition(partitions.get(i - 1), TAU_SortedList, attributeValuesList, numberTuples);
            //chLevel1.setElementCount(spLevel1.getElementCount());
            //chLevel1.setError(spLevel1.getError());
            //level1.put(combinationLevel1, chLevel1);
            
        }
        //End Initialize Level 1

        // DEBUG
        /*System.out.println("TAU_SortedList");
        for(ObjectBigArrayBigList<LongBigArrayBigList> e : TAU_SortedList)
        	System.out.println(e);
        // DEBUG
        System.out.println("attributeValuesList");
        for(ObjectBigArrayBigList<Integer> e : attributeValuesList)
        	System.out.println(e);
        // DEBUG
        System.out.println("Partitions");
        for(int i = 0; i < partitions.size(); i++)
        	System.out.println(partitions.get(i));*/
        
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
        System.out.println("Time(s) Before While Loop : " + (end1 - start1)/1000);

        int L = 1;
        while (!level1.isEmpty() && L <= numberAttributes) {
        	curr_level = L;
            //compute dependencies for a level

            System.out.println("LEVEL : " + L + " size : " + level1.size() + " # FD : " + numberOfFD + " # OD : " + numberOfOD);

            computeODs2(L, b_table);

            // prune the search space
            if(Prune)
                prune(L);

            // compute the combinations for the next level
            generateNextLevel(b_table);
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

        Collections.sort(FDODScoreList, FDODScore2.FDODScoreComparator());

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

    private static void generateNextLevel(Broadcast<int[][]> b_table) {
    	//OD
        level_minus1 = level0;
    	
    	level0 = level1;
        level1 = null;

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
        
        //spComputation = spComputation + pruned_list.size();
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
        level1 = new_level;
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
    private static void computeODs(int L, Broadcast<int[][]> b_table) {

    	// Broadcast these for later use
    	final Broadcast<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> b_level0 = sc.broadcast(level0);
        final Broadcast<ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>>> b_TAU_SortedList = sc.broadcast(TAU_SortedList);
        final Broadcast<ArrayList<ObjectBigArrayBigList<Integer>>> b_attributeValuesList = sc.broadcast(attributeValuesList);
        final Broadcast<Boolean> b_BidirectionalTrue = sc.broadcast(BidirectionalTrue);
        final Broadcast<Boolean> b_BidirectionalPruneTrue = sc.broadcast(BidirectionalPruneTrue);
        //final Broadcast<Integer> b_curr_level = sc.broadcast(curr_level);
        final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
        final Broadcast<Long> b_numberTuples = sc.broadcast(numberTuples);

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
                long spXwithoutACount = level0.get(Xclone).getUniqueCount();
                long spXCount = level1.get(X).getUniqueCount();

                if (spXCount == spXwithoutACount) {

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
	
	                    ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition_X_minus_AB = null;
	                    //String pkg1_combination_name = OpenBitSetToBitSetString(X_minus_AB, b_numberAttributes.value());
	    				//pkg1_combination_name = pkg1_combination_name.replaceAll(", ", "-").replaceAll("\\{", "").replaceAll("\\}", "");
	    				
	    				//if(pkg1_combination_name.equals("")) {
	    				//	strippedPartition_X_minus_AB = new StrippedPartition(b_numberTuples.value()).getStrippedPartition();
	                    //} else {
		                    strippedPartition_X_minus_AB = new ObjectBigArrayBigList<LongBigArrayBigList>();
		                    HashMap<ArrayList<Integer>, LongBigArrayBigList> hashSet = new HashMap<ArrayList<Integer>, LongBigArrayBigList>();
		        			//int[][] table = b_table.value();
		        			for(int i = 0; i < b_table.value().length; i++){
		        				int[] row = b_table.value()[i];
		        				ArrayList<Integer> value = new ArrayList<Integer>();
		        				for(int j = 1; j <= b_numberAttributes.value(); j++) {
		            				if(X_minus_AB.get(j))
		            					value.add(row[j-1]);
		            			}
		        				if(!hashSet.containsKey(value))
		        					hashSet.put(value, new LongBigArrayBigList());
		        				hashSet.get(value).add(i);
		        			}
		        			for(ArrayList<Integer> key : hashSet.keySet()) {
		        				if(hashSet.get(key).size64() > 1) {
		        					strippedPartition_X_minus_AB.add(hashSet.get(key));
		        				}
		        			}
	                    //}
	        			
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
    
    private static void computeODs2(int L, Broadcast<int[][]> b_table) {

    	// Broadcast these for later use
    	final Broadcast<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> b_level0 = sc.broadcast(level0);
        final Broadcast<ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>>> b_TAU_SortedList = sc.broadcast(TAU_SortedList);
        final Broadcast<ArrayList<ObjectBigArrayBigList<Integer>>> b_attributeValuesList = sc.broadcast(attributeValuesList);
        final Broadcast<Boolean> b_BidirectionalTrue = sc.broadcast(BidirectionalTrue);
        final Broadcast<Boolean> b_BidirectionalPruneTrue = sc.broadcast(BidirectionalPruneTrue);
        //final Broadcast<Integer> b_curr_level = sc.broadcast(curr_level);
        final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
        final Broadcast<Long> b_numberTuples = sc.broadcast(numberTuples);

        initializeCplus_c_ForLevel(); //Line 2 in Algorithm 3
        //OD
        initializeCplus_s_ForLevel(L);

        // iterate through the combinations of the level
        for (OpenBitSet X : level1.keySet()) {
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
                long spXwithoutACount = level0.get(Xclone).getUniqueCount();
                long spXCount = level1.get(X).getUniqueCount();

                if (spXCount == spXwithoutACount) {
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
        }
        
        ArrayList<OpenBitSet> level1List = new ArrayList<OpenBitSet>();
        for(OpenBitSet bs : level1.keySet()) {
        	level1List.add(bs);
        }
        JavaRDD<OpenBitSet> level1RDD = sc.parallelize(level1List);
        final Broadcast<Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>> b_level1 = sc.broadcast(level1);
        //for (OpenBitSet X : level1.keySet()) {
        JavaRDD<ArrayList<Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>>> prunedSwapCandidatesRDD = 
        		level1RDD.map(new Function<OpenBitSet, ArrayList<Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>>>(){
        	public ArrayList<Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>> call(OpenBitSet X){
            //*************************** ORDER DEPENDENCIES (CANONICAL FORM 2)
            ArrayList<OpenBitSet> removeFromC_s_List = new ArrayList<OpenBitSet>();
            ArrayList<OpenBitSet> swapCandidatesList = new ArrayList<OpenBitSet>();
            ArrayList<Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>> resultList = 
            		new ArrayList<Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>>();
            //final Broadcast<OpenBitSet> b_X = sc.broadcast(X);
            
            /*for(OpenBitSet bs : level1.get(X).getSwapCandidates()) {
            	swapCandidatesList.add(bs);
            }
            JavaRDD<OpenBitSet> swapCandidatesRDD = sc.parallelize(swapCandidatesList);*/
            
            //SwapCandidates is C_s_plus
            //JavaRDD<Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>> prunedSwapCandidatesRDD = swapCandidatesRDD.map(new Function<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>(){
            //	public Tuple3<OpenBitSet, OpenBitSet, OpenBitSet> call(OpenBitSet oneAB){
            for(OpenBitSet oneAB : b_level1.value().get(X).getSwapCandidates()){
            		Tuple3<OpenBitSet, OpenBitSet, OpenBitSet> result = null;
	                //line 18, Algorithm 3
	                OpenBitSet[] A_B_Separate = getSeparateOpenBitSet_AB(oneAB, b_numberAttributes.value());
	                int[] A_B_Index = getIndexOfOpenBitSet_AB(oneAB, b_numberAttributes.value());
	
	                OpenBitSet A = A_B_Separate[0];
	                OpenBitSet B = A_B_Separate[1];
	
	                int A_index = A_B_Index[0]; //starts from 1
	                int B_index = A_B_Index[1]; //starts from 1
	
	                OpenBitSet X_minus_A = (OpenBitSet) X.clone();
	                X_minus_A.remove(A);
	                OpenBitSet C_c_X_minus_A = b_level0.value().get(X_minus_A).getRhsCandidates();
	                OpenBitSet C_c_X_minus_A_Clone = C_c_X_minus_A.clone();
	                C_c_X_minus_A_Clone.union(B);
	
	                OpenBitSet X_minus_B = (OpenBitSet) X.clone();
	                X_minus_B.remove(B);
	                OpenBitSet C_c_X_minus_B = b_level0.value().get(X_minus_B).getRhsCandidates();
	                OpenBitSet C_c_X_minus_B_Clone = C_c_X_minus_B.clone();
	                C_c_X_minus_B_Clone.union(A);
	
	                //this is exactly the if statement in line 18
	                if(  !(C_c_X_minus_B.equals(C_c_X_minus_B_Clone)) ||  !(C_c_X_minus_A.equals(C_c_X_minus_A_Clone))){
	                    //removeFromC_s_List.add(oneAB);
	                	result = new Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>(oneAB, null, null);
	                }else{
	                    //step 1: find X\{A,B}
	                    OpenBitSet X_minus_AB = (OpenBitSet) X.clone();
	                    X_minus_AB.remove(A);
	                    X_minus_AB.remove(B);
	
	                    ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition_X_minus_AB = null;
	                    strippedPartition_X_minus_AB = new ObjectBigArrayBigList<LongBigArrayBigList>();
	                    HashMap<ArrayList<Integer>, LongBigArrayBigList> hashSet = new HashMap<ArrayList<Integer>, LongBigArrayBigList>();
	        			for(int i = 0; i < b_table.value().length; i++){
	        				int[] row = b_table.value()[i];
	        				ArrayList<Integer> value = new ArrayList<Integer>();
	        				for(int j = 1; j <= b_numberAttributes.value(); j++) {
	            				if(X_minus_AB.get(j))
	            					value.add(row[j-1]);
	            			}
	        				if(!hashSet.containsKey(value))
	        					hashSet.put(value, new LongBigArrayBigList());
	        				hashSet.get(value).add(i);
	        			}
	        			for(ArrayList<Integer> key : hashSet.keySet()) {
	        				if(hashSet.get(key).size64() > 1) {
	        					strippedPartition_X_minus_AB.add(hashSet.get(key));
	        				}
	        			}
	        			
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
	                        result = new Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>(oneAB, X_minus_AB, oneAB);
	                    }
	                }
	                resultList.add(new Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>(X, result));
	            }
            return resultList;
        	}
        	});
            List<ArrayList<Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>>> prunedSwapCandidatesList = prunedSwapCandidatesRDD.collect();
            //remove ABs
            if(Prune) {
            	for(ArrayList<Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>>> smallList : prunedSwapCandidatesList) {
            		for(Tuple2<OpenBitSet, Tuple3<OpenBitSet, OpenBitSet, OpenBitSet>> t : smallList){
            			Tuple3<OpenBitSet, OpenBitSet, OpenBitSet> triple = t._2;
            			OpenBitSet X = t._1;
	                	if(triple == null)
	                		continue;
	                	OpenBitSet removedAB = triple._1();
	                    level1.get(X).getSwapCandidates().remove(removedAB);
	                    if(triple._2() != null && triple._3() != null) {
	                    	numberOfOD ++;
	                    	//printOpenBitSetNames("OD:", triple._2(), triple._3());
	                    }
            		}
            	}
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

class FDODScore2{
    public long score;
    public OpenBitSet X_minus_AB;
    public OpenBitSet oneAB;
    public FunctionalDependency functionalDependency;

    public FDODScore2(long score, OpenBitSet X_minus_AB, OpenBitSet oneAB){
        score = score;
        X_minus_AB = X_minus_AB;
        oneAB = oneAB;
        functionalDependency = null;
    }

    public FDODScore2(long score, FunctionalDependency functionalDependency){
        score = score;
        X_minus_AB = null;
        oneAB = null;
        functionalDependency = functionalDependency;
    }

    public static Comparator<FDODScore2> FDODScoreComparator(){

        Comparator comp = new Comparator<FDODScore2>(){
            public int compare(FDODScore2 s1, FDODScore2 s2){
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
