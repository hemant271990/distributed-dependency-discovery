package hyfd_impl;
/*
 * This is an optimized implementation,
 * modifications:
 * a) does not compute comparison suggestions,
 * b) Sampling phase: sample non-FDs from randomly partitioned data 
 */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import hyfd_helper.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import scala.Tuple2;

public class DistributedHyFD2 implements Serializable {

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
	public static int numPartitions = 100; // #of horizontal data partitions
	public static int samplerBatchSize = 66;
	public static int validatorBatchSize = 2000;
	public static int num_spark_task_factor = 2;
	
	public static void execute(){
		long startTime = System.currentTimeMillis();
		valueComparator = new ValueComparator(true);
		executeHyFD();
		Logger.getInstance().writeln("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	public static void executeHyFD(){
		// Initialize
		Logger.getInstance().writeln("Loading data...");
		loadData();
		maxLhsSize = numberAttributes - 1;
		///////////////////////////////////////////////////////
		// Build data structures for sampling and validation //
		///////////////////////////////////////////////////////
		
		// Calculate plis
		
		//PLIBuilder pliBuilder = new PLIBuilder(numberTuples);
		//List<PositionListIndex> plis = pliBuilder.getPLIs(tuples, numberAttributes, valueComparator.isNullEqualNull());
		//JavaRDD<PositionListIndex> plisRDD = buildPLIs();
		//plisRDD.cache();
		
		// DEBUG
		/*List<PositionListIndex> plis = plisRDD.collect();
		for(int i = 0; i < plis.size(); i++)
		{
			System.out.println("pli "+plis.get(i).getAttribute()+" | "+plis.get(i).toString());
		}*/
				
		//final int numRecords = pliBuilder.getNumLastRecords();
		//pliBuilder = null;
		final int numRecords = new Long(numberTuples).intValue();

		//JavaPairRDD<Integer, Integer[]> compressedRecordsRDD = generateRecords(); //generateCompressedRecords(invertedPlisRDD);
		//compressedRecordsRDD.cache();
		/*Map<Integer, Integer[]> compressedRecordsMap = compressedRecordsRDD.collectAsMap();
		System.out.println("Compressed Records");
		for(int i = 0; i < compressedRecordsMap.size(); i++)
		{
			System.out.print("i= "+i+" | ");
			for(int j = 0; j < compressedRecordsMap.get(i).length; j++)
			{
				System.out.print(compressedRecordsMap.get(i)[j]+" ");
			}
			System.out.println();
		}*/
		
		// Initialize the negative cover
		FDSet negCover = new FDSet(numberAttributes, maxLhsSize);
		
		// Initialize the positive cover
		FDTree posCover = new FDTree(numberAttributes, maxLhsSize);
		posCover.addMostGeneralDependencies();
		
		//////////////////////////
		// Build the components //
		//////////////////////////

		// TODO: implement parallel sampling
		DistributedSampler2 sampler = new DistributedSampler2(negCover, efficiencyThreshold, samplerBatchSize, numberAttributes, numRecords, numPartitions, df, sc);
		Inductor inductor = new Inductor(negCover, posCover, memoryGuardian);
		DistributedValidator validator = new DistributedValidator(posCover, efficiencyThreshold, numberAttributes, validatorBatchSize, sc, df, num_spark_task_factor);
		
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		do {
			FDList newNonFds = sampler.enrichNegativeCover();
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
			
			inductor.updatePositiveCover(newNonFds);
			//System.out.println(" #### of FDs: " + posCover.writeFunctionalDependencies(new OpenBitSet(), buildColumnIdentifiers(), false));
			comparisonSuggestions = validator.validatePositiveCover();
		}
		while (comparisonSuggestions != null);
		negCover = null;
		
		// Output all valid FDs
		Logger.getInstance().writeln("Translating FD-tree into result format ...");
		int numFDs = posCover.writeFunctionalDependencies(new OpenBitSet(), buildColumnIdentifiers(), false);
		Logger.getInstance().writeln("... done! (" + numFDs + " FDs)");
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

	
	private static JavaRDD<PositionListIndex> buildPLIs() {
		JavaRDD<PositionListIndex> plisRDD = sc.emptyRDD();
		JavaPairRDD<Integer, Iterable<IntArrayList>> bigRDD = sc.parallelizePairs(new ArrayList<Tuple2<Integer, Iterable<IntArrayList>>>());
		final Broadcast<Integer> b_numberTuples = sc.broadcast(new Long(numberTuples).intValue());
        JavaRDD<Row> datasetRDD = df.javaRDD();
        for(int i = 0; i < numberAttributes; i++) {
        	
        	final Broadcast<Integer> b_attribute = sc.broadcast(i);
        	
        	JavaPairRDD<Long, IntArrayList> pairRDD = datasetRDD.mapToPair(
        			new PairFunction<Row, Long, IntArrayList>() {
        				private static final long serialVersionUID = 1L;
		        		public Tuple2<Long, IntArrayList> call(Row r) {
		        			IntArrayList l = new IntArrayList();
		        			l.add(new Long(r.getLong(r.size()-1)).intValue());
		        			return new Tuple2<Long, IntArrayList>(r.getLong(b_attribute.value()), l);
		        		}
        	});
        	
        	JavaPairRDD<Long, IntArrayList> strippedPartitionPairRDD = pairRDD.reduceByKey(
        			new Function2<IntArrayList, IntArrayList, IntArrayList>() {
        				private static final long serialVersionUID = 1L;
        				public IntArrayList call(IntArrayList l1, IntArrayList l2) {
        					l1.addAll(l2);
        					return l1;
        				};
        	});
        	
        	JavaPairRDD<Integer, Iterable<IntArrayList>> strippedPartitionsRDD = strippedPartitionPairRDD.mapToPair(
        			new PairFunction<Tuple2<Long, IntArrayList>, Integer, IntArrayList> () {
        				private static final long serialVersionUID = 1L;
        				public Tuple2<Integer, IntArrayList> call(Tuple2<Long, IntArrayList> t) {
        					if(t._2.size() <= 1)
        						return new Tuple2<Integer, IntArrayList>(b_attribute.value(), null);
        					Collections.sort(t._2); // This might be costly and not required.
        					return new Tuple2<Integer, IntArrayList>(b_attribute.value(), t._2);
        				}
        	}).groupByKey();
        	
        	bigRDD = bigRDD.union(strippedPartitionsRDD);
        }
        
        plisRDD = bigRDD.map(
        		new Function<Tuple2<Integer, Iterable<IntArrayList>>, PositionListIndex>() {
        			private static final long serialVersionUID = 1L;
	        		public PositionListIndex call(Tuple2<Integer, Iterable<IntArrayList>> t) {
	        			List<IntArrayList> l = new ArrayList<IntArrayList>();
	        			for(IntArrayList il : t._2) {
	        				if(il != null)
	        					l.add(il);
	        			}
	        			return new PositionListIndex(t._1, l);
	        		}
        });
        
        plisRDD = plisRDD.sortBy(new Function<PositionListIndex, Integer>(){
        	private static final long serialVersionUID = 1L;
        	public Integer call(PositionListIndex p) {
        		return b_numberTuples.value() - p.getNumNonUniqueValues() + p.getClusters().size();
        	}
        }, false, numPartitions);
        
        return plisRDD;
	}
	
	private static JavaPairRDD<Integer, Integer[]> generateCompressedRecords(JavaPairRDD<Integer, Integer[]> invertedPlisRDD)
	{
		final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> elementBlowUpRDD = invertedPlisRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Integer, Integer[]>, Integer, Tuple2<Integer, Integer>>() {
					private static final long serialVersionUID = 1L;
					public Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> call(Tuple2<Integer, Integer[]> t) {
						List<Tuple2<Integer, Tuple2<Integer, Integer>>> res = new ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>>();
						int innerKey = t._1;
						for(int i = 0; i < t._2.length; i++) {
							int outerKey = i;
							int e = t._2[i];
							Tuple2<Integer, Tuple2<Integer, Integer>> tres = new Tuple2<Integer, Tuple2<Integer, Integer>>(outerKey, new Tuple2<Integer, Integer>(innerKey, e));
							res.add(tres);
						}
						return res.iterator();
					}
		});
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> groupByColumn = elementBlowUpRDD.groupByKey();
		JavaPairRDD<Integer, Integer[]> transposedRDD = groupByColumn.mapToPair(
				new PairFunction<Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>, Integer, Integer[]> () {
					private static final long serialVersionUID = 1L;
					public Tuple2<Integer, Integer[]> call(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> t) {
						Integer[] arr = new Integer[b_numberAttributes.value()];
						int key = t._1;
						for(Tuple2<Integer, Integer> t_in : t._2) {
							arr[t_in._1] = t_in._2;
						}
						return new Tuple2<Integer, Integer[]>(key, arr);
					}
			
		}).sortByKey(true);
		
		return transposedRDD;
	}
	
	// Table as RDD, not like above where it is table of cluster IDs
	private static JavaPairRDD<Integer, Integer[]> generateRecords()
	{
		JavaRDD<Row> dataRDD = df.javaRDD();
		
		final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
		
		JavaPairRDD<Integer, Integer[]> tableRDD = dataRDD.mapToPair(new PairFunction<Row, Integer, Integer[]>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<Integer, Integer[]> call(Row r) {
				Integer[] arr = new Integer[b_numberAttributes.value()];
				for(int i = 0; i < b_numberAttributes.value(); i++)
					arr[i] = new Long(r.getLong(i)).intValue();
				Integer key = new Long(r.getLong(b_numberAttributes.value())).intValue();
				return new Tuple2<Integer, Integer[]>(key, arr);
			}
		});//.sortByKey(true);
		
		return tableRDD;
	}
}
