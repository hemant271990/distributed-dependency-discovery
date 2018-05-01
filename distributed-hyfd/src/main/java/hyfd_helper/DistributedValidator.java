package hyfd_helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

import hyfd_helper.FDTree;
import hyfd_helper.FDTreeElement;
import hyfd_helper.FDTreeElementLhsPair;
import hyfd_helper.IntegerPair;
import scala.Tuple2;
import hyfd_helper.Logger;

public class DistributedValidator {

	private FDTree posCover;
	private float efficiencyThreshold;
	private static HashMap<OpenBitSet, Integer> level0_unique = new HashMap<OpenBitSet, Integer>();
	private static HashMap<OpenBitSet, Integer> level1_unique = new HashMap<OpenBitSet, Integer>();
	private static int spComputation = 0;
	private static int numberAttributes;
	public static int batch_size = 1;
	public static JavaSparkContext sc;
	public static Dataset<Row> df = null;
	public static int numSparkTasksFactor = 1;
	
	private int level = 0;

	public DistributedValidator(FDTree posCover,float efficiencyThreshold, int numAttributes, 
			int batchSize, JavaSparkContext sparkContext, Dataset<Row> dataFrame, int num_spark_task_factor) {
		this.posCover = posCover;
		this.efficiencyThreshold = 10.0f;
		numberAttributes = numAttributes;
		batch_size = batchSize;
		sc = sparkContext;
		df = dataFrame;
		numSparkTasksFactor = num_spark_task_factor;
	}
	
	private class FD {
		public OpenBitSet lhs;
		public int rhs;
		public FD(OpenBitSet lhs, int rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
		}
	}
	
	private class ValidationResult {
		public int validations = 0;
		public int intersections = 0;
		public List<FD> invalidFDs = new ArrayList<>();
		public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		public void add(ValidationResult other) {
			this.validations += other.validations;
			this.intersections += other.intersections;
			this.invalidFDs.addAll(other.invalidFDs);
			this.comparisonSuggestions.addAll(other.comparisonSuggestions);
		}
	}
	
	/*private class ValidationTask implements Callable<ValidationResult> {
		private FDTreeElementLhsPair elementLhsPair;
		private final Map<Integer, Integer[]> compressedRecords;
		public void setElementLhsPair(FDTreeElementLhsPair elementLhsPair) {
			this.elementLhsPair = elementLhsPair;
		}
		public ValidationTask(FDTreeElementLhsPair elementLhsPair, Map<Integer, Integer[]> compressedRecords) {
			this.compressedRecords = compressedRecords;
			this.elementLhsPair = elementLhsPair;
		}
		public ValidationResult call() throws Exception {
			ValidationResult result = new ValidationResult();
			
			FDTreeElement element = this.elementLhsPair.getElement();
			OpenBitSet lhs = this.elementLhsPair.getLhs();
			OpenBitSet rhs = element.getFds();
			
			int rhsSize = (int) rhs.cardinality();
			if (rhsSize == 0)
				return result;
			result.validations = result.validations + rhsSize;
			
			if (DistributedValidator.this.level == 0) {
				// Check if rhs is unique
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					if (!DistributedValidator.this.plis.get(rhsAttr).isConstant(DistributedValidator.this.numRecords)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					}
					result.intersections++;
				}
			}
			else if (DistributedValidator.this.level == 1) {
				// Check if lhs from plis refines rhs
				int lhsAttribute = lhs.nextSetBit(0);
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					if (!DistributedValidator.this.plis.get(lhsAttribute).refines(compressedRecords, rhsAttr)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					}
					result.intersections++;
				}
			}
			else {
				// Check if lhs from plis plus remaining inverted plis refines rhs
				int firstLhsAttr = lhs.nextSetBit(0);
				
				lhs.clear(firstLhsAttr);
				OpenBitSet validRhs = DistributedValidator.this.plis.get(firstLhsAttr).refines(compressedRecords, lhs, rhs, result.comparisonSuggestions);
				lhs.set(firstLhsAttr);
				
				result.intersections++;
				
				rhs.andNot(validRhs); // Now contains all invalid FDs
				element.setFds(validRhs); // Sets the valid FDs in the FD tree
				
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
					result.invalidFDs.add(new FD(lhs, rhsAttr));
			}
			return result;
		}
	}*/

	/*private ValidationResult validateSequential(List<FDTreeElementLhsPair> currentLevel, Map<Integer, Integer[]> compressedRecords) {
		ValidationResult validationResult = new ValidationResult();
		
		ValidationTask task = new ValidationTask(null, compressedRecords);
		for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			task.setElementLhsPair(elementLhsPair);
			try {
				validationResult.add(task.call());
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return validationResult;
	}*/
	
	private ValidationResult validateSpark(List<FDTreeElementLhsPair> currentLevel, Broadcast<int[][]> b_table) {
		ValidationResult result = new ValidationResult();
		
		HashSet<OpenBitSet> uniquesToCompute = new HashSet<OpenBitSet>();
		ArrayList<int[]> combination_arr = new ArrayList<int[]>();
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
        
        System.out.println(" Total uniques to compute l0 - l1: "+l0_count+" "+l1_count);
		// compute uniques in batches via spark
        LinkedList<OpenBitSet> batch = new LinkedList<OpenBitSet>();
        for(OpenBitSet bs : uniquesToCompute) {
            /*if(!level0_unique.containsKey(bs)) {
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
            }*/
        	spComputation++;
            batch.add(bs);
            full++;

            if(full == batch_size || l == uniquesToCompute.size()-1) { // then process the batch
            	//System.out.println(" Starting Spark job at level: "+level + " for batch size: "+combination_arr.size());
            	JavaRDD<OpenBitSet> combinationsRDD = sc.parallelize(batch);
                Map<String, Integer> map = generateStrippedPartitions(combinationsRDD, b_table);
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
	            if(level0_unique.containsKey(lhs) && level1_unique.containsKey(lhsNrhs) &&
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
	
	// Works best, least size
    public static Map<String, Integer> generateStrippedPartitions6(ArrayList<int[]> combinations, int num_combinations) {
    	final Broadcast<ArrayList<int[]>> b_combinations = sc.broadcast(combinations);
    	final Broadcast<Integer> b_num_combinations = sc.broadcast(num_combinations);
    	JavaRDD<Row> dataRDD = df.javaRDD();
    	
    	// Row = <C0, C1, C2, ... , index>
    	
    	JavaRDD<ArrayList<Integer>> combValueRDD = dataRDD.repartition(660*numSparkTasksFactor).flatMap(new FlatMapFunction<Row, ArrayList<Integer>>() {
    		public Iterator<ArrayList<Integer>> call(Row r) {
    			HashSet<ArrayList<Integer>> hash = new HashSet<ArrayList<Integer>>();
    			for(int i = 0; i < b_num_combinations.value(); i++) {
    				ArrayList<Integer> l = new ArrayList<Integer>();
    				l.add(i);
					for(int j = 0; j < b_combinations.value().get(i).length; j++) {
						l.add((new Long(r.getLong(b_combinations.value().get(i)[j]))).intValue());
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
    			int[] combArr = b_combinations.value().get(idx);
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
    
	/*private ValidationResult validateParallel(List<FDTreeElementLhsPair> currentLevel, Map<Integer, Integer[]> compressedRecords) {
		ValidationResult validationResult = new ValidationResult();
		
		List<Future<ValidationResult>> futures = new ArrayList<>();
		for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			ValidationTask task = new ValidationTask(elementLhsPair, compressedRecords);
			futures.add(this.executor.submit(task));
		}
		
		for (Future<ValidationResult> future : futures) {
			try {
				validationResult.add(future.get());
			}
			catch (ExecutionException e) {
				this.executor.shutdownNow();
				e.printStackTrace();
			}
			catch (InterruptedException e) {
				this.executor.shutdownNow();
				e.printStackTrace();
			}
		}
		
		return validationResult;
	}*/
	
	public List<IntegerPair> validatePositiveCover(Broadcast<int[][]> b_table) {
		int numAttributes = numberAttributes;
		
		//Logger.getInstance().writeln("Validating FDs using plis ...");
		
		List<FDTreeElementLhsPair> currentLevel = null;
		if (this.level == 0) {
			currentLevel = new ArrayList<>();
			currentLevel.add(new FDTreeElementLhsPair(this.posCover, new OpenBitSet(numAttributes)));
		}
		else {
			currentLevel = this.posCover.getLevel(this.level);
		}
		
		// Start the level-wise validation/discovery
		int previousNumInvalidFds = 0;
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		while (!currentLevel.isEmpty()) {
			Logger.getInstance().write("\tLevel " + this.level + ": " + currentLevel.size() + " elements; ");
			
			// Validate current level
			Logger.getInstance().write("(V)");
			
			//ValidationResult validationResult = (this.executor == null) ? this.validateSequential(currentLevel, compressedRecords) : this.validateParallel(currentLevel, compressedRecords);
			ValidationResult validationResult = this.validateSpark(currentLevel, b_table);
			
			comparisonSuggestions.addAll(validationResult.comparisonSuggestions);
			
			// If the next level exceeds the predefined maximum lhs size, then we can stop here
			if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth())) {
				int numInvalidFds = validationResult.invalidFDs.size();
				int numValidFds = validationResult.validations - numInvalidFds;
				Logger.getInstance().writeln("(-)(-); " + validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + "-" + " new candidates; --> " + numValidFds + " FDs");
				break;
			}
			
			// Add all children to the next level
			Logger.getInstance().write("(C)");
			
			List<FDTreeElementLhsPair> nextLevel = new ArrayList<>();
			for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
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
			}
						
			// Generate new FDs from the invalid FDs and add them to the next level as well
			Logger.getInstance().write("(G); ");
			
			int candidates = 0;
			for (FD invalidFD : validationResult.invalidFDs) {
				for (int extensionAttr = 0; extensionAttr < numAttributes; extensionAttr++) {
					OpenBitSet childLhs = this.extendWith(invalidFD.lhs, invalidFD.rhs, extensionAttr);
					if (childLhs != null) {
						FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(childLhs, invalidFD.rhs);
						if (child != null) {
							nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
							candidates++;
						}
					}
				}
				
				if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth()))
					break;
			}
			
			currentLevel = nextLevel;
			level0_unique = level1_unique;
            level1_unique = new HashMap<OpenBitSet, Integer>();
			this.level++;
			int numInvalidFds = validationResult.invalidFDs.size();
			int numValidFds = validationResult.validations - numInvalidFds;
			Logger.getInstance().writeln(validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");
		
			// Decide if we continue validating the next level or if we go back into the sampling phase
			//if ((numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds))
			if (numInvalidFds > numValidFds * this.efficiencyThreshold)
				return comparisonSuggestions;
			//	return new ArrayList<>();
			previousNumInvalidFds = numInvalidFds;
		}
		
		return null;
	}
	
	private OpenBitSet extendWith(OpenBitSet lhs, int rhs, int extensionAttr) {
		if (lhs.get(extensionAttr) || 											// Triviality: AA->C cannot be valid, because A->C is invalid
			(rhs == extensionAttr) || 											// Triviality: AC->C cannot be valid, because A->C is invalid
			this.posCover.containsFdOrGeneralization(lhs, extensionAttr) ||		// Pruning: If A->B, then AB->C cannot be minimal // TODO: this pruning is not used in the Inductor when inverting the negCover; so either it is useless here or it is useful in the Inductor?
			((this.posCover.getChildren() != null) && (this.posCover.getChildren()[extensionAttr] != null) && this.posCover.getChildren()[extensionAttr].isFd(rhs)))	
																				// Pruning: If B->C, then AB->C cannot be minimal
			return null;
		
		OpenBitSet childLhs = lhs.clone(); // TODO: This clone() could be avoided when done externally
		childLhs.set(extensionAttr);
		
		// TODO: Add more pruning here
		
		// if contains FD: element was a child before and has already been added to the next level
		// if contains Generalization: element cannot be minimal, because generalizations have already been validated
		if (this.posCover.containsFdOrGeneralization(childLhs, rhs))										// Pruning: If A->C, then AB->C cannot be minimal
			return null;
		
		return childLhs;
	}

}
