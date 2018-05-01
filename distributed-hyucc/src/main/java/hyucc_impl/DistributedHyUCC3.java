package hyucc_impl;
/*
 * This is an optimized implementation,
 * modifications:
 * a) does not compute comparison suggestions,
 * b) Sampling phase: sample non-FDs from randomly partitioned data
 * c) switches between sampling and validation based on cost.
 */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import hyucc_helper.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import scala.Tuple2;

public class DistributedHyUCC3 implements Serializable {

	/*public enum Identifier {
		INPUT_GENERATOR, NULL_EQUALS_NULL, VALIDATE_PARALLEL, ENABLE_MEMORY_GUARDIAN, MAX_DETERMINANT_SIZE, INPUT_ROW_LIMIT
	};*/

	//private static ValueComparator valueComparator;
	private static final MemoryGuardian memoryGuardian = new MemoryGuardian(true);
	
	private static int maxLhsSize = -1;				// The lhss can become numAttributes - 1 large, but usually we are only interested in FDs with lhs < some threshold (otherwise they would not be useful for normalization, key discovery etc.)
	private static float efficiencyThreshold = 0.0001f;
	
	public static Dataset<Row> df = null;
	public static JavaSparkContext sc;
	public static String datasetFile = null;
	public static String[] columnNames = null;
	public static long numberTuples = 0;
	public static Integer numberAttributes = 0;
	public static int numPartitions = 55; // #of horizontal data partitions
	//public static int samplerBatchSize = 66;
	//public static int validatorBatchSize = 2000;
	//public static int num_spark_task_factor = 2;
	private static Map<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cartesianPartition = null;
	private static int posInCartesianPartition = 0;
	public static int batchSize = 55;
	public static int validationBatchSize = 5000;
	private static UCCSet negCover = null;
	private static UCCTree posCover = null;
	private static HashMap<OpenBitSet, Integer> level0_unique = new HashMap<OpenBitSet, Integer>();
	//private static HashMap<OpenBitSet, Integer> level1_unique = new HashMap<OpenBitSet, Integer>();
	private static int level = 0;
	private static int spComputation = 0;
	private static int numNewNonFds = 0;
	public static long genEQClassTime = 0;
	public static long diffJoinTime = 0;
	public static int numUCCs = 0;
	//public static Inductor inductor = null;
	private static long startTime = 0;
	private static int totalNonFDs = 0;
	
	public static void execute(){
		startTime = System.currentTimeMillis();
		//valueComparator = new ValueComparator(true);
		executeHyFD();
		System.out.println(" genEQClassTime time(s): " + genEQClassTime/1000);
        System.out.println(" diffJoin time(s): " + diffJoinTime/1000);
		Logger.getInstance().writeln("Time: " + (System.currentTimeMillis() - startTime)/1000 + "s");
	}

	public static void executeHyFD(){
		// Initialize
		Logger.getInstance().writeln("Loading data...");
		loadData();
		long t1 = System.currentTimeMillis();
		partitionData();
		long t2 = System.currentTimeMillis();
		diffJoinTime += t2-t1;
		maxLhsSize = numberAttributes-1;

		// Broadcast the table
		int[][] table = new int[new Long(numberTuples).intValue()][numberAttributes];
    	int currRow = 0;
    	List<Row> rowList = df.collectAsList();
    	for(Row r : rowList) {
    		for(int i = 0; i < numberAttributes; i++) {
    			table[currRow][i] = new Long(r.getLong(i)).intValue();
    		}
    		currRow++;
    	}
    	final Broadcast<int[][]> b_table = sc.broadcast(table);
    	
		negCover = new UCCSet(numberAttributes, maxLhsSize);
		posCover = new UCCTree(numberAttributes, maxLhsSize);
		posCover.addMostGeneralUniques();
		//inductor = new Inductor(negCover, posCover, memoryGuardian);
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		
		while(!posCover.getLevel(level).isEmpty()/*!level1_unique.isEmpty()*/ && level <= numberAttributes) {
			validatePositiveCover(b_table);
			
			//UCCList newNonFds = enrichNegativeCover(b_table);
			//System.out.println(" #### of non FDs: " + newNonFds.size());
			
			// DEBUG
			/*System.out.println(" Print NegCover: ");
			for(ObjectOpenHashSet<OpenBitSet> hs : negCover.getUccLevels()) {
				for(OpenBitSet b : hs) {
					for(int i = 0; i < posCover.getNumAttributes(); i++)
						if(b.get(i))
							System.out.print(i+" ");
					System.out.println();
				}
			}*/
			
			//updatePositiveCover(newNonFds);
			//System.out.println(" #### of FDs: " + posCover.writeFunctionalDependencies(new OpenBitSet(), buildColumnIdentifiers(), false));
			
		}
		negCover = null;
		
		// Output all valid FDs
		Logger.getInstance().writeln("Translating UCC-tree into result format ...");
		int numUCCs = posCover.addUniqueColumnCombinationsInto(new OpenBitSet(), buildColumnIdentifiers(), null);
		Logger.getInstance().writeln("... done! (" + numUCCs + " UCCs)");
	}

	private static ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(columnNames.length);
		for (String attributeName : columnNames)
			columnIdentifiers.add(new ColumnIdentifier(datasetFile, attributeName));
		return columnIdentifiers;
	}
	
	private static void loadData() {
        df.cache();
        numberTuples = df.count();
    }

	
	/**
     * Random partitioning of data.
     */
    private static void partitionData() {
    	
    	final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
    	
    	JavaRDD<Row> tableRDD = df.javaRDD();
    	JavaPairRDD<Integer, Integer> rowMap = tableRDD.mapToPair(
    			new PairFunction<Row, Integer, Integer>() {
	    		  public Tuple2<Integer, Integer> call(Row r) { 
	    			  return new Tuple2<Integer, Integer>((int)(Math.random()*b_numPartitions.value()), new Long(r.getLong(r.size()-1)).intValue()); 
	    			  }
    		});
    	JavaPairRDD<Integer, Iterable<Integer>> pairsPartition = rowMap.groupByKey();
    	JavaPairRDD<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>> joinedPairs = pairsPartition.cartesian(pairsPartition);
    	joinedPairs = joinedPairs.filter(new Function<Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>>, Boolean> () {
    		public Boolean call(Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>> t) {
    			if(t._1._1 > t._2._1)
    				return false;
    			return true;
    		}
    	});
    	// (1: {{1,2,3},{4,5,6}}; 2: {{1,2,3},{7,8,9}})
    	cartesianPartition = joinedPairs.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>>, Iterable<Integer>, Iterable<Integer>>(){
    		public Tuple2<Iterable<Integer>, Iterable<Integer>> call(Tuple2<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, Iterable<Integer>>> t) {
    			return new Tuple2<Iterable<Integer>, Iterable<Integer>>(t._1._2, t._2._2);
    		}
    	}).zipWithIndex().mapToPair(new PairFunction<Tuple2<Tuple2<Iterable<Integer>, Iterable<Integer>>, Long>, Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>(){
    		public Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> call(Tuple2<Tuple2<Iterable<Integer>, Iterable<Integer>>, Long> t){
    			return new Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>(t._2.intValue(), t._1);
    		}
    	}).collectAsMap();
    	
    	/*// DEBUG
    	Map<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> tmp = cartesianPartition.collectAsMap();
    	for(Integer key: tmp.keySet()) {
    		System.out.println(" ***** "+ key);
    		System.out.println("LEFT:");
    		for(Row r : tmp.get(key)._1)
    			System.out.println(r);
    		System.out.println("RIGHT:");
    		for(Row r : tmp.get(key)._2)
    			System.out.println(r);
    	}*/
    }
	
    public static UCCList enrichNegativeCover(final Broadcast<int[][]> b_table) {
		Logger.getInstance().writeln("Enriching Negative Cover ... ");
		
		long t1 = System.currentTimeMillis();
		UCCList newNonFds = runNext(/*newNonFds,*/ negCover, batchSize, b_table);
		long t2 = System.currentTimeMillis();
		diffJoinTime += t2-t1;
		return newNonFds;
	}
	
	public static UCCList runNext(/*UCCList newNonFds,*/ UCCSet negCover, int batchSize, final Broadcast<int[][]> b_table) {
		UCCList newNonFds = new UCCList(numberAttributes, maxLhsSize);
		int previousNegCoverSize = newNonFds.size();
		
		long numComparisons = 0;
		
    	final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
    	List<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>> currentMap = new ArrayList<Tuple2<Integer, Tuple2< Iterable<Integer>, Iterable<Integer>>>>();
    	
    	for(int i = posInCartesianPartition; i < posInCartesianPartition + batchSize; i++) {
    		if(i >= cartesianPartition.size())
    			break;
    		currentMap.add(new Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>(i, cartesianPartition.get(i)));
    	}
    	JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> currentJobRDD = sc.parallelizePairs(currentMap, numPartitions);
    	
    	// DEBUG
    	/*Map<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> tmp = currentJobRDD.collectAsMap();
    	for(Integer key: tmp.keySet()) {
    		System.out.println(" ***** "+ key);
    		System.out.println("LEFT:");
    		for(Integer r : tmp.get(key)._1)
    			System.out.println(r);
    		System.out.println("RIGHT:");
    		for(Integer r : tmp.get(key)._2)
    			System.out.println(r);
    	}*/
    	
    	JavaRDD<HashSet<OpenBitSet>> nonFDsHashetRDD = currentJobRDD.map(
				new Function<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>, HashSet<OpenBitSet>>() {
					public HashSet<OpenBitSet> call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> t) {
						HashSet<OpenBitSet> result_l = new HashSet<OpenBitSet>();
    					//FDTree negCoverTree = new FDTree(b_numberAttributes.value());
    					Iterable<Integer> l1 = t._2._1;
    					Iterable<Integer> l2 = t._2._2;
    					for(int i1 : l1) {
    						int[] r1 = b_table.value()[i1];
    						for(int i2 : l2) {
    							int[] r2 = b_table.value()[i2];
    							OpenBitSet equalAttrs = new OpenBitSet(b_numberAttributes.value());
    							equalAttrs.clear(0, b_numberAttributes.value());
    							for (int i = 0; i < b_numberAttributes.value(); i++) {
    								int val1 = r1[i];
    					            int val2 = r2[i];
    								if (val1 >= 0 && val2 >= 0 && val1 == val2) {
    									equalAttrs.set(i);
    								}
    							}
    					        result_l.add(equalAttrs);
    						}
    					}
    					return result_l;
					}
		});
    	
    	JavaRDD<OpenBitSet> nonFDsRDD = nonFDsHashetRDD.flatMap(new FlatMapFunction<HashSet<OpenBitSet>, OpenBitSet>(){
    		public Iterator<OpenBitSet> call(HashSet<OpenBitSet> hs) {
    			return hs.iterator();
    		}
    	}).distinct();
		
    	// DEBUG
		/*System.out.println(" Print NegCover: ");
		List<OpenBitSet> l = nonFDsRDD.collect();
		for(OpenBitSet b : l) {
			for(int i = 0; i < numberAttributes; i++)
				if(b.get(i))
					System.out.print(i+" ");
			System.out.println();
		}*/
    	
		for(OpenBitSet nonFD : nonFDsRDD.collect()) {
			if (!negCover.contains(nonFD)) {
				//System.out.println(" Add the above to negCover");
				OpenBitSet equalAttrsCopy = nonFD.clone();
				negCover.add(equalAttrsCopy);
				newNonFds.add(equalAttrsCopy);
			}
		}
		
		posInCartesianPartition = posInCartesianPartition + batchSize;
		
		int partitionSize = (int) (numberTuples/numPartitions);
		numComparisons = (long) partitionSize*partitionSize*batchSize;
		numNewNonFds = newNonFds.size() - previousNegCoverSize;
		float efficiency = (float) numNewNonFds/numComparisons;
		totalNonFDs += numNewNonFds;
		System.out.println("new NonFDs: "+numNewNonFds+" # comparisons: "+numComparisons+" TOTAL: "+totalNonFDs+" efficiency threshold: "+efficiencyThreshold);
		return newNonFds;
	}
	
	public static void validatePositiveCover(final Broadcast<int[][]> b_table) {
		int numAttributes = numberAttributes;

		List<UCCTreeElementUCCPair> currentLevel = null;
		if (level == 0) {
			currentLevel = new ArrayList<UCCTreeElementUCCPair>();
			currentLevel.add(new UCCTreeElementUCCPair(posCover, new OpenBitSet(numAttributes)));
		}
		else {
			currentLevel = posCover.getLevel(level);
		}
		
		// DEBUG
		/*System.out.println(" Print PosCover: ");
		for(int l = 0; l < numberAttributes; l++){
			for(UCCTreeElementUCCPair hs : posCover.getLevel(l)) {
				OpenBitSet b = hs.getUCC();
				for(int i = 0; i < posCover.getNumAttributes(); i++)
					if(b.get(i))
						System.out.print(i+" ");
				System.out.println();
				
			}
		}*/
		
		// Start the level-wise validation/discovery
		int previousNumInvalidFds = 0;
		Logger.getInstance().write("\tLevel " + level + ": " + currentLevel.size() + " elements; ");
		
		// Validate current level
		Logger.getInstance().write("(V)");
		
		ValidationResult validationResult = validateSpark(currentLevel, b_table);
		
		if(validationResult == null)
			return;
		
		// Add all children to the next level
		Logger.getInstance().write("(C)");
		
		List<UCCTreeElementUCCPair> nextLevel = new ArrayList<>();
		/*for (UCCTreeElementUCCPair elementLhsPair : currentLevel) {
			UCCTreeElement element = elementLhsPair.getElement();
			OpenBitSet ucc = elementLhsPair.getUCC();

			if (element.getChildren() == null)
				continue;
			
			for (int childAttr = 0; childAttr < numAttributes; childAttr++) {
				UCCTreeElement child = element.getChildren()[childAttr];
				
				if (child != null) {
					OpenBitSet childUCC = ucc.clone();
					childUCC.set(childAttr);
					nextLevel.add(new UCCTreeElementUCCPair(child, childUCC));
				}
			}
			
		}*/
		//System.out.println("NEXT LEVEL SIZE 1: " + nextLevel.size());			
		// Generate new FDs from the invalid FDs and add them to the next level as well
		Logger.getInstance().write("(G); ");
		
		int candidates = 0;
		for (OpenBitSet invalidUCC : validationResult.invalidUCCs) {
			for (int extensionAttr = 0; extensionAttr < numAttributes; extensionAttr++) {
				OpenBitSet childUCC = extendWith(invalidUCC, extensionAttr);
				if (childUCC != null) {
					//System.out.println("VALID CHILD");
					UCCTreeElement child = posCover.addUniqueColumnCombinationGetIfNew(childUCC);
					if (child != null) {
						//nextLevel.add(new UCCTreeElementUCCPair(child, childUCC));
						candidates++;
					}
				} else {
					//System.out.println("RETURN NULL");
				}
			}
			
			/*if ((posCover.getMaxDepth() > -1) && (level >= posCover.getMaxDepth()))
				break;*/
		}
		//System.out.println("NEXT LEVEL SIZE 2: " + nextLevel.size());	
		
		//currentLevel = nextLevel;
        level0_unique = new HashMap<OpenBitSet, Integer>();
		level++;
		int numInvalidUCCs = validationResult.invalidUCCs.size();
		int numValidUCCs = validationResult.validations - numInvalidUCCs;
		numUCCs += numValidUCCs;
		Logger.getInstance().writeln(validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidUCCs + " invalid; " + candidates + " new candidates; --> " + numValidUCCs + " UCCs");
		previousNumInvalidFds = numInvalidUCCs;
		long time = System.currentTimeMillis();
		System.out.println("Total UCCs so far: " + numUCCs);
		System.out.println("----Time elapse: " + (time-startTime)/1000);
		return;
	}
	
	private static ValidationResult validateSpark(List<UCCTreeElementUCCPair> currentLevel, Broadcast<int[][]> b_table) {
		long t1 = System.currentTimeMillis();
		ValidationResult result = new ValidationResult();
		
		HashSet<OpenBitSet> uniquesToCompute = new HashSet<OpenBitSet>();
		ArrayList<int[]> combination_arr = new ArrayList<int[]>();
        int full = 0; // checks how many entries added to combination_arr
        int l = 0;
        
        int l0_count = 0;
        int l1_count = 0;
        // Find out what uniques to compute
        for(UCCTreeElementUCCPair elementUCCPair : currentLevel) {
        	UCCTreeElement element = elementUCCPair.getElement();
			OpenBitSet ucc = elementUCCPair.getUCC();
			if(element.isUCC())
				uniquesToCompute.add(ucc);
			l0_count++;
			
        }
        long t2 = System.currentTimeMillis();
        genEQClassTime += t2 - t1;
        // Continue only if validation phase is cheaper, else return
        System.out.println("\n Uniques to compute: "+ uniquesToCompute.size() + " Sampling load: "+numberTuples/numPartitions);
        if(uniquesToCompute.size() > numberTuples/numPartitions) {
        	UCCList newNonFds = enrichNegativeCover(b_table);
			
        	// Check if newNonFDs were zero then going to sampling phase use useless, go ahead with validation
        	if(numNewNonFds != 0) {
        		updatePositiveCover(newNonFds);
        		return null;
        	}
        }
        long t3 = System.currentTimeMillis();	
        System.out.println(" Total uniques to compute l0 - l1: "+l0_count+" "+l1_count);
		// compute uniques in batches via spark
        LinkedList<OpenBitSet> batch = new LinkedList<OpenBitSet>();
        for(OpenBitSet bs : uniquesToCompute) {
            spComputation++;
            batch.add(bs);
            full++;

            if(full == validationBatchSize || l == uniquesToCompute.size()-1) { // then process the batch
            	System.out.println("Running Spark job for batch size: "+batch.size());
		        JavaRDD<OpenBitSet> combinationsRDD = sc.parallelize(batch);
                Map<String, Integer> map = generateStrippedPartitions(combinationsRDD, b_table);
                Iterator<Entry<String, Integer>> entry_itr = map.entrySet().iterator();
                while(entry_itr.hasNext()){
                    Entry<String, Integer> e = entry_itr.next();
                    //System.out.println(e.getKey() + " count: " + e.getValue());
                    OpenBitSet combination = stringToBitset(e.getKey());
                    /*if(combination.cardinality() == level+1) {
                        level1_unique.put(combination, e.getValue());
                        System.out.println("Adding to level1");
                    } else if(combination.cardinality() == level){*/
                        level0_unique.put(combination, e.getValue());
                    //}
                }
                full = 0;
                combination_arr = new ArrayList<int[]>();
                batch = new LinkedList<OpenBitSet>();
            }
            l++;
        }
            
        /* Now we have all unique counts required to calculate FDs at this level (=key)
         * Begin validating FDs, if something is not a FD add its specifications to the candidates
         * After validation for this level then, assign level1_unique to level0_unique, 
         * and clear level1_unique for next iteration. 
         */
        
        for(UCCTreeElementUCCPair elementLhsPair : currentLevel) {
        	ValidationResult localResult = new ValidationResult();
        	
        	UCCTreeElement element = elementLhsPair.getElement();
			OpenBitSet ucc = elementLhsPair.getUCC();
			localResult.validations = localResult.validations + 1;
			
			if(level0_unique.containsKey(ucc)){
				if (level0_unique.get(ucc).intValue() != numberTuples) {
					element.setUCC(false);
					localResult.invalidUCCs.add(ucc);
				}
			} /*else {
				System.out.println("LEVEL 0 does not contain");
			}*/
				//System.out.println(" Key discovered: "+ ucc);
				//element.setUCC(true);
				
			//element.setFds(validRhs); // Sets the valid FDs in the FD tree
			localResult.intersections++;
			
			result.add(localResult);
        }
        long t4 = System.currentTimeMillis();
        genEQClassTime += t4 - t3;
		return result;
	}
	
	public static Map<String, Integer> generateStrippedPartitions(JavaRDD<OpenBitSet> combinationsRDD, final Broadcast<int[][]> b_table) {
    	final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
    	JavaPairRDD<String, Integer> attrSpRDD2 = combinationsRDD.mapToPair(new PairFunction<OpenBitSet, String, Integer>(){
    		public Tuple2<String, Integer> call(OpenBitSet b) {
    			HashSet<ArrayList<Integer>> hashSet = new HashSet<ArrayList<Integer>>();
    			String combination = "";
    			int[][] table = b_table.value();
    			for(int i = 0; i < b_numberAttributes.value(); i++) {
    				if(b.get(i))
    					combination += "_"+i;
    			}
    			for(int i = 0; i < table.length; i++){
    				int[] row = table[i];
    				ArrayList<Integer> value = new ArrayList<Integer>();
    				for(int j = 0; j < b_numberAttributes.value(); j++) {
        				if(b.get(j))
        					value.add(row[j]);
        			}
    				hashSet.add(value);
    			}
    			return new Tuple2<String, Integer>(combination, hashSet.size());
    		}
    	});
    	return attrSpRDD2.collectAsMap();
	}
	
	/*private static OpenBitSet extendWith(OpenBitSet lhs, int rhs, int extensionAttr) {
		if (lhs.get(extensionAttr) || 											// Triviality: AA->C cannot be valid, because A->C is invalid
			(rhs == extensionAttr) || 											// Triviality: AC->C cannot be valid, because A->C is invalid
			posCover.containsFdOrGeneralization(lhs, extensionAttr) ||		// Pruning: If A->B, then AB->C cannot be minimal // TODO: this pruning is not used in the Inductor when inverting the negCover; so either it is useless here or it is useful in the Inductor?
			((posCover.getChildren() != null) && (posCover.getChildren()[extensionAttr] != null) && posCover.getChildren()[extensionAttr].isFd(rhs)))	
																				// Pruning: If B->C, then AB->C cannot be minimal
			return null;
		
		OpenBitSet childLhs = lhs.clone(); // TODO: This clone() could be avoided when done externally
		childLhs.set(extensionAttr);
		
		// TODO: Add more pruning here
		
		// if contains FD: element was a child before and has already been added to the next level
		// if contains Generalization: element cannot be minimal, because generalizations have already been validated
		if (posCover.containsFdOrGeneralization(childLhs, rhs))										// Pruning: If A->C, then AB->C cannot be minimal
			return null;
		
		return childLhs;
	}*/
	
	private static OpenBitSet extendWith(OpenBitSet ucc, int extensionAttr) {
		if (ucc.get(extensionAttr))
			return null;
		
		OpenBitSet childUCC = ucc.clone();
		childUCC.set(extensionAttr);
		
		/*for(int i = 0; i < posCover.getNumAttributes(); i++)
			if(childUCC.get(i))
				System.out.print(i+" ");
		System.out.println();*/
		
		if (posCover.containsUCCOrGeneralization(childUCC)) {
			//System.out.println("INVALID CHILD");
			return null;
		}
		
//		if (this.negCover.containsUCCOrSpecialization(childUCC)) // TODO: May be needed?
//			return null;
		
		return childUCC;
	}
	
	public static void updatePositiveCover(UCCList nonUCCs) {
		Logger.getInstance().writeln("Inducing UCC candidates ..." + nonUCCs.size());
		for (int i = nonUCCs.getUccLevels().size() - 1; i >= 0; i--) {
			if (i >= nonUCCs.getUccLevels().size()) // If this level has been trimmed during iteration
				continue;
			
			List<OpenBitSet> nonUCCLevel = nonUCCs.getUccLevels().get(i);
			for (OpenBitSet nonUCC : nonUCCLevel)
				specializePositiveCover(nonUCC, nonUCCs);
			nonUCCLevel.clear();
		}
	}
	
	protected static int specializePositiveCover(OpenBitSet nonUCC, UCCList nonUCCs) {
		int numAttributes = posCover.getChildren().length;
		int newUCCs = 0;
		List<OpenBitSet> specUCCs;
		
		if (!(specUCCs = posCover.getUCCAndGeneralizations(nonUCC)).isEmpty()) { // TODO: May be "while" instead of "if"?
			for (OpenBitSet specUCC : specUCCs) {
				posCover.removeUniqueColumnCombination(specUCC);
				/*if ((posCover.getMaxDepth() > 0) && (specUCC.cardinality() >= posCover.getMaxDepth()))
					continue;*/
				
				for (int attr = numAttributes - 1; attr >= 0; attr--) {
					if (!nonUCC.get(attr)) {
						specUCC.set(attr);
						if (!posCover.containsUCCOrGeneralization(specUCC)) {
							posCover.addUniqueColumnCombination(specUCC);
							newUCCs++;	
							
							// If dynamic memory management is enabled, frequently check the memory consumption and trim the positive cover if it does not fit anymore
							/*this.memoryGuardian.memoryChanged(1);
							this.memoryGuardian.match(this.negCover, this.posCover, nonUCCs);*/
						}
						specUCC.clear(attr);
					}
				}
			}
		}
		return newUCCs;
	}
	
	private static OpenBitSet stringToBitset(String str) {
		OpenBitSet bs = new OpenBitSet(numberAttributes);
		String[] splitArr = str.split("_");
		for(int i = 0; i < splitArr.length; i++) {
			if(!splitArr[i].equals("")){
				int pos = Integer.parseInt(splitArr[i]);
				bs.set(pos);
			}
		}
		return bs;
	}
	
}
