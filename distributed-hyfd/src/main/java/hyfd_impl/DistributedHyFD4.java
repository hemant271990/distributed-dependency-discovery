package hyfd_impl;
/*
 * This is an optimized implementation,
 * modifications:
 * a) does not compute comparison suggestions,
 * b) Sampling phase: sample non-FDs from randomly partitioned data
 * c) switches between sampling and validation based on cost.
 * d) No broadcast of data. Meant for small memory workers (smPDP)
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
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import hyfd_helper.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class DistributedHyFD4 implements Serializable {

	public enum Identifier {
		INPUT_GENERATOR, NULL_EQUALS_NULL, VALIDATE_PARALLEL, ENABLE_MEMORY_GUARDIAN, MAX_DETERMINANT_SIZE, INPUT_ROW_LIMIT
	};

	private static ValueComparator valueComparator;
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
	public static int numSubPartitions = 1;
	public static int reps = 0;
	//public static int samplerBatchSize = 66;
	//public static int validatorBatchSize = 2000;
	//public static int num_spark_task_factor = 2;
	//private static Map<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cartesianPartition = null;
	//private static JavaPairRDD<Integer, Tuple2<Iterable<Row>, Iterable<Row>>> cartesianPartition = null;
	private static JavaPairRDD<Integer, ArrayList<Row>> pairsPartition = null;
    private static HashMap<Integer, ArrayList<Integer>> subPartitionMap = new HashMap<Integer, ArrayList<Integer>>();
	private static int posInCartesianPartition = 0;
	public static int batchSize = 55;
	public static int validationBatchSize = 5000;
	private static FDSet negCover = null;
	private static FDTree posCover = null;
	private static HashMap<OpenBitSet, Integer> level0_unique = new HashMap<OpenBitSet, Integer>();
	private static HashMap<OpenBitSet, Integer> level1_unique = new HashMap<OpenBitSet, Integer>();
	private static int level = 0;
	private static int spComputation = 0;
	private static int numNewNonFds = 0;
	public static long genEQClassTime = 0;
	public static long diffJoinTime = 0;
	public static Inductor inductor = null;
	
	public static void execute(){
		long startTime = System.currentTimeMillis();
		valueComparator = new ValueComparator(true);
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
		maxLhsSize = numberAttributes - 1;
		//final int numRecords = new Long(numberTuples).intValue();

		// Broadcast the table
		/*int[][] table = new int[new Long(numberTuples).intValue()][numberAttributes];
    	int currRow = 0;
    	List<Row> rowList = getChunk(0, 100);
    	for(Row r : rowList) {
    		for(int i = 0; i < numberAttributes; i++) {
    			System.out.print(r.getLong(i)+" ");
    			table[currRow][i] = new Long(r.getLong(i)).intValue();
    		}
    		System.out.println();
    		currRow++;
    	}
    	final Broadcast<int[][]> b_table = sc.broadcast(table);*/
    	
		negCover = new FDSet(numberAttributes, maxLhsSize);
		posCover = new FDTree(numberAttributes, maxLhsSize);
		posCover.addMostGeneralDependencies();
		inductor = new Inductor(negCover, posCover, memoryGuardian);
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		
		// Bootstrap with some non-FDs before starting the main algorithm
		for(int i = 0; i < 1; i++) {
			System.out.println("Bootstraping round: "+i);
			FDList newNonFds = enrichNegativeCover();
			inductor.updatePositiveCover(newNonFds);
		}
    	
		while(!posCover.getLevel(level).isEmpty()/*!level1_unique.isEmpty()*/ && level <= numberAttributes) {
			validatePositiveCover();
			
			//FDList newNonFds = enrichNegativeCover(b_table);
			//System.out.println(" #### of non FDs: " + newNonFds.size());
			
			/*// DEBUG
			System.out.println(" Print NegCover: ");
			for(ObjectOpenHashSet<OpenBitSet> hs : negCover.getFdLevels()) {
				for(OpenBitSet b : hs) {
					for(int i = 0; i < posCover.getNumAttributes(); i++)
						if(b.get(i))
							System.out.print(i+" ");
					System.out.println();
				}
			}*/
			
			//inductor.updatePositiveCover(newNonFds);
			//System.out.println(" #### of FDs: " + posCover.writeFunctionalDependencies(new OpenBitSet(), buildColumnIdentifiers(), false));
			
		}
		negCover = null;
		
		// Output all valid FDs
		Logger.getInstance().writeln("Translating FD-tree into result format ...");
		int numFDs = posCover.writeFunctionalDependencies(new OpenBitSet(), buildColumnIdentifiers(), false);
		Logger.getInstance().writeln("... done! (" + numFDs + " FDs)");
	}

	private static List<Row> getChunk(long start, long end) {
		final Broadcast<Long> b_start = sc.broadcast(start);
		final Broadcast<Long> b_end = sc.broadcast(end);
		List<Row> rowList = df.filter(new FilterFunction<Row>() {
			@Override
			public boolean call(Row r) throws Exception {
				if(r.getLong(0) >= b_start.value() && r.getLong(0) < b_end.value())
					return true;
				else
					return false;
			}
		}).collectAsList();
		return rowList;
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
    	numSubPartitions = numPartitions/10;
    	int key = 0;
        for(int i = 0; i < numSubPartitions; i++) {
        	for(int j = i; j < numSubPartitions; j++) {
        		ArrayList<Integer> l = new ArrayList<Integer>();
        		l.add(i);
        		l.add(j);
        		subPartitionMap.put(key++, l);
        	}
        }
        
        final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
    	
    	JavaRDD<Row> tableRDD = df.javaRDD().repartition(numPartitions);
    	JavaPairRDD<Integer, Row> rowMap = tableRDD.mapToPair(
    			new PairFunction<Row, Integer, Row>() {
	    		  public Tuple2<Integer, Row> call(Row r) { 
	    			  return new Tuple2<Integer, Row>((int)(Math.random()*b_numPartitions.value()), r); 
	    			  }
    		});
    	pairsPartition = rowMap.groupByKey().mapToPair(
    			new PairFunction<Tuple2<Integer,Iterable<Row>>, Integer, ArrayList<Row>>() {
		    		public Tuple2<Integer, ArrayList<Row>> call(Tuple2<Integer,Iterable<Row>> t) {
		    			Iterable<Row> rl = t._2;
		    			ArrayList<Row> list = new ArrayList<Row>();
		    			for(Row r : rl)
		    				list.add(r);
		    			return new Tuple2<Integer, ArrayList<Row>>(t._1, list);
		    		}
    	});
    	pairsPartition.cache();
    	System.out.println("Data partitioned...");
    }
	
    public static FDList enrichNegativeCover() {
		Logger.getInstance().writeln("Enriching Negative Cover ... ");
		FDList newNonFds = new FDList(numberAttributes, negCover.getMaxDepth());
		long t1 = System.currentTimeMillis();
		runNext(newNonFds, negCover, batchSize, reps++);
		long t2 = System.currentTimeMillis();
		diffJoinTime += t2-t1;
		return newNonFds;
	}
	
	/*public static void runNext(FDList newNonFds, FDSet negCover, int batchSize, final Broadcast<int[][]> b_table) {
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
    							for (int i = 0; i < b_numberAttributes.value(); i++) {
    								long val1 = r1[i];
    					            long val2 = r2[i];
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
		System.out.println("new NonFDs: "+numNewNonFds+" # comparisons: "+numComparisons+" efficiency: "+efficiency+" efficiency threshold: "+efficiencyThreshold);
	}*/
	
	
	public static void runNext(FDList newNonFds, FDSet negCover, int batchSize, int key) {
		int previousNegCoverSize = newNonFds.size();
		long numComparisons = 0;
		
		if(key >= subPartitionMap.size()){
			numNewNonFds = 0;
			return;
		}
			
		final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
    	final Broadcast<Integer> b_numSubPartitions = sc.broadcast(numSubPartitions);
		final Broadcast<Integer> b_key = sc.broadcast(key);
		final Broadcast<HashMap<Integer, ArrayList<Integer>>> b_subPartitionMap = sc.broadcast(subPartitionMap);
    	JavaRDD<HashSet<OpenBitSet>> nonFDsHashetRDD = pairsPartition.map(
    			new Function<Tuple2<Integer, ArrayList<Row>>, HashSet<OpenBitSet>>() {
    				public HashSet<OpenBitSet> call(Tuple2<Integer, ArrayList<Row>> t) {
    					HashSet<OpenBitSet> result_l = new HashSet<OpenBitSet>();
    					ArrayList<Row> rl0 = new ArrayList<Row>();
    					ArrayList<Row> rl1 = new ArrayList<Row>();
    					int part0 = b_subPartitionMap.value().get(b_key.value()).get(0);
    					int part1 = b_subPartitionMap.value().get(b_key.value()).get(1);
    					int sampleSize = t._2.size()/b_numSubPartitions.value();
    					int start0 = (part0*sampleSize) % t._2.size();
    					int start1 = (part1*sampleSize) % t._2.size();
    					int count = 0;
    					while(count < sampleSize) {
    						rl0.add(t._2.get(start0++));
    						count++;
    					}
    					
    					count = 0;
    					while(count < sampleSize) {
    						rl1.add(t._2.get(start1++));
    						count++;
    					}
    					
    					for(Row r0 : rl0) {
    						for(Row r1 : rl1) {
    							
    							OpenBitSet equalAttrs = new OpenBitSet(b_numberAttributes.value());
    					        for (int i = 0; i < b_numberAttributes.value(); i++) {
    					            long val0 = r0.getLong(i);
    					            long val1 = r1.getLong(i);
    					            // Handling of null values. Currently assuming NULL values are equal.
    					            if (val0 >= 0 && val1 >= 0 && val0 == val1) {
    					                // OpenBitSet start with 1 for first attribute
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
		System.out.println("new NonFDs: "+numNewNonFds+" # comparisons: "+numComparisons+" efficiency: "+efficiency+" efficiency threshold: "+efficiencyThreshold);
	}
	
	
	public static void validatePositiveCover() {
		int numAttributes = numberAttributes;

		List<FDTreeElementLhsPair> currentLevel = null;
		if (level == 0) {
			currentLevel = new ArrayList<>();
			currentLevel.add(new FDTreeElementLhsPair(posCover, new OpenBitSet(numAttributes)));
		}
		else {
			currentLevel = posCover.getLevel(level);
		}
		
		// Start the level-wise validation/discovery
		int previousNumInvalidFds = 0;
		Logger.getInstance().write("\tLevel " + level + ": " + currentLevel.size() + " elements; ");
		
		// Validate current level
		Logger.getInstance().write("(V)");
		
		//ValidationResult validationResult = (this.executor == null) ? this.validateSequential(currentLevel, compressedRecords) : this.validateParallel(currentLevel, compressedRecords);
		// The cost check of Validation vs Sampling happens inside validateSpark
		//long t1 = System.currentTimeMillis();
		ValidationResult validationResult = validateSpark(currentLevel);
		//long t2 = System.currentTimeMillis();
		//genEQClassTime += t2 - t1;
		
		if(validationResult == null)
			return;
		
		// If the next level exceeds the predefined maximum lhs size, then we can stop here
		/*if ((posCover.getMaxDepth() > -1) && (level >= posCover.getMaxDepth())) {
			int numInvalidFds = validationResult.invalidFDs.size();
			int numValidFds = validationResult.validations - numInvalidFds;
			Logger.getInstance().writeln("(-)(-); " + validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + "-" + " new candidates; --> " + numValidFds + " FDs");
			return;
		}*/
		
		// Add all children to the next level
		Logger.getInstance().write("(C)");
		
		List<FDTreeElementLhsPair> nextLevel = new ArrayList<>();
		/*for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			FDTreeElement element = elementLhsPair.getElement();
			OpenBitSet lhs = elementLhsPair.getLhs();

			if (element.getChildren() == null)
				continue;
			
			for (int childAttr = 0; childAttr < numAttributes; childAttr++) {
				FDTreeElement child = element.getChildren()[childAttr];
				
				if (child != null) {
					OpenBitSet childLhs = lhs.clone();
					childLhs.set(childAttr);
					nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
				}
			}
		}*/
					
		// Generate new FDs from the invalid FDs and add them to the next level as well
		Logger.getInstance().write("(G); ");
		
		int candidates = 0;
		for (FD invalidFD : validationResult.invalidFDs) {
			for (int extensionAttr = 0; extensionAttr < numAttributes; extensionAttr++) {
				OpenBitSet childLhs = extendWith(invalidFD.lhs, invalidFD.rhs, extensionAttr);
				if (childLhs != null) {
					FDTreeElement child = posCover.addFunctionalDependencyGetIfNew(childLhs, invalidFD.rhs);
					if (child != null) {
						nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
						candidates++;
					}
				}
			}
			
			/*if ((posCover.getMaxDepth() > -1) && (level >= posCover.getMaxDepth()))
				break;*/
		}
		
		currentLevel = nextLevel;
		level0_unique = level1_unique;
        level1_unique = new HashMap<OpenBitSet, Integer>();
		level++;
		int numInvalidFds = validationResult.invalidFDs.size();
		int numValidFds = validationResult.validations - numInvalidFds;
		Logger.getInstance().writeln(validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");
		previousNumInvalidFds = numInvalidFds;
		
		return;
	}
	
	private static ValidationResult validateSpark(List<FDTreeElementLhsPair> currentLevel) {
		long t1 = System.currentTimeMillis();
		ValidationResult result = new ValidationResult();
		
		HashSet<OpenBitSet> uniquesToCompute = new HashSet<OpenBitSet>();
        int full = 0; // checks how many entries added to combination_arr
        int l = 0;
        
        int l0_count = 0;
        int l1_count = 0;
        // Find out what uniques to compute
        for(FDTreeElementLhsPair elementLhsPair : currentLevel) {
        	FDTreeElement element = elementLhsPair.getElement();
			OpenBitSet lhs = elementLhsPair.getLhs();
			OpenBitSet rhs = element.getFds();
			if(!level0_unique.containsKey(lhs)) {
				uniquesToCompute.add(lhs);
				l0_count++;
			}
			
			for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
				OpenBitSet lhsRhs = lhs.clone();
				lhsRhs.set(rhsAttr);
				uniquesToCompute.add(lhsRhs);
				l1_count++;
			}
        }
        long t2 = System.currentTimeMillis();
        genEQClassTime += t2 - t1;
        // Continue only if validation phase is cheaper, else return
        System.out.println("\n Uniques to compute: "+ uniquesToCompute.size() + " Sampling load: "+numberTuples/numPartitions);
        if(uniquesToCompute.size() > numberTuples/numPartitions) {
        	FDList newNonFds = enrichNegativeCover();
			
        	// Check if newNonFDs were zero then going to sampling phase use useless, go ahead with validation
        	if(numNewNonFds != 0) {
        		inductor.updatePositiveCover(newNonFds);
        		return null;
        	}
        }
        long t3 = System.currentTimeMillis();	
        //System.out.println(" Total uniques to compute l0 - l1: "+l0_count+" "+l1_count);
		// compute uniques in batches via spark
        //LinkedList<OpenBitSet> batch = new LinkedList<OpenBitSet>();
        ArrayList<int[]> combination_arr = new ArrayList<int[]>();
        for(OpenBitSet bs : uniquesToCompute) {
        	//if(!level0_unique.containsKey(bs)) {
                spComputation++;
                
                int cardinality = (new Long(bs.cardinality())).intValue();
                int[] bs_arr = new int[cardinality];
                int m = 0;
                for(int i = 0; i < numberAttributes; i++) {
                    if(bs.get(i)) {
                    	//System.out.print(i);
                        bs_arr[m++] = i;
                    }
                }
                //System.out.print(" ");
                combination_arr.add(bs_arr);
                full++;
            //}
            

            if(full == validationBatchSize || l == uniquesToCompute.size()-1) { // then process the batch
            	System.out.println("Running Spark job for batch size: "+combination_arr.size());
		        //JavaRDD<OpenBitSet> combinationsRDD = sc.parallelize(batch);
            	long t4 = System.currentTimeMillis();
                Map<String, Integer> map = generateStrippedPartitionsGroupBy(combination_arr, full);
                System.out.println("  -- time: "+(System.currentTimeMillis() - t4)/1000);
                Iterator<Entry<String, Integer>> entry_itr = map.entrySet().iterator();
                while(entry_itr.hasNext()){
                    Entry<String, Integer> e = entry_itr.next();
                    //System.out.println(e.getKey() + " count: " + e.getValue());
                    OpenBitSet combination = stringToBitset(e.getKey());
                    if(combination.cardinality() == level+1) {
                        level1_unique.put(combination, e.getValue());
                    } else if(combination.cardinality() == level){
                        level0_unique.put(combination, e.getValue());
                    }
                }
                full = 0;
                combination_arr = new ArrayList<int[]>();
            }
            l++;
        }
            
        /* Now we have all unique counts required to calculate FDs at this level (=key)
         * Begin validating FDs, if something is not a FD add its specifications to the candidates
         * After validation for this level then, assign level1_unique to level0_unique, 
         * and clear level1_unique for next iteration. 
         */
        
        for(FDTreeElementLhsPair elementLhsPair : currentLevel) {
        	ValidationResult localResult = new ValidationResult();
        	
        	FDTreeElement element = elementLhsPair.getElement();
			OpenBitSet lhs = elementLhsPair.getLhs();
			OpenBitSet rhs = element.getFds();
			OpenBitSet validRhs = new OpenBitSet(numberAttributes);
			
			int rhsSize = (int) rhs.cardinality();
			localResult.validations = localResult.validations + rhsSize;
			
			for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
				OpenBitSet lhsNrhs = lhs.clone();
				lhsNrhs.set(rhsAttr);
				
				//System.out.println(level0_unique.get(lhs) +" "+ level1_unique.get(lhsNrhs));
	            if(/*level0_unique.containsKey(lhs) && level1_unique.containsKey(lhsNrhs) &&*/
	                    level0_unique.get(lhs).intValue() == level1_unique.get(lhsNrhs).intValue()){
	            	validRhs.set(rhsAttr);
	            } else {
	            	localResult.invalidFDs.add(new FD(lhs, rhsAttr));
	            }
			}
			
			element.setFds(validRhs); // Sets the valid FDs in the FD tree
			localResult.intersections++;
			
			result.add(localResult);
        }
        long t4 = System.currentTimeMillis();
        genEQClassTime += t4 - t3;
		return result;
	}
	
	public static Map<String, Integer> generateStrippedPartitionsGroupBy(ArrayList<int[]> combinations, int num_combinations) {
    	Map<String, Integer> result = new HashMap<String, Integer>();
    	for(int i = 0; i < num_combinations; i++){
    		List<String> comb = new ArrayList<String>();
    		String combStr = "";
    		for(int j = 0; j < combinations.get(i).length; j++) {
    			comb.add(df.columns()[combinations.get(i)[j]]);
    			combStr += "_"+combinations.get(i)[j];
    			//System.out.println(i+" "+j+" "+combinations[i][j]+" "+ df.columns()[combinations[i][j]-1]);
    		}
    		if(comb.isEmpty())
    			continue;
    		String firstC = comb.remove(0);
    		Seq<String> combSeq = JavaConverters.asScalaIteratorConverter(comb.iterator()).asScala().toSeq();
    		Long count = df.agg(functions.countDistinct(firstC, combSeq)).collectAsList().get(0).getLong(0);
    		//System.out.println(" Count for "+firstC+" "+count);
    		result.put(combStr, count.intValue());
    	}
    	return result;
    }
	
	private static OpenBitSet extendWith(OpenBitSet lhs, int rhs, int extensionAttr) {
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
