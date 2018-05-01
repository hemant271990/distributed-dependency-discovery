package hyfd_impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import hyfd_helper.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

public class HyFD implements Serializable {

	public enum Identifier {
		INPUT_GENERATOR, NULL_EQUALS_NULL, VALIDATE_PARALLEL, ENABLE_MEMORY_GUARDIAN, MAX_DETERMINANT_SIZE, INPUT_ROW_LIMIT
	};

	//private FunctionalDependencyResultReceiver resultReceiver = null;

	private static ValueComparator valueComparator;
	private static final MemoryGuardian memoryGuardian = new MemoryGuardian(true);
	
	private static boolean validateParallel = true;	// The validation is the most costly part in HyFD and it can easily be parallelized
	private static int maxLhsSize = -1;				// The lhss can become numAttributes - 1 large, but usually we are only interested in FDs with lhs < some threshold (otherwise they would not be useful for normalization, key discovery etc.)
	
	private static float efficiencyThreshold = 0.01f;
	
	public static Dataset<Row> df = null;
	public static JavaSparkContext sc;
	public static String datasetFile = null;
	public static List<List<String>> tuples = null;
	public static String[] columnNames = null;
	public static long numberTuples = 0;
	public static Integer numberAttributes = 0;

	/*public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}*/

	/*@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
		if (HyFD.Identifier.NULL_EQUALS_NULL.name().equals(identifier))
			this.valueComparator = new ValueComparator(values[0].booleanValue());
		else if (HyFD.Identifier.VALIDATE_PARALLEL.name().equals(identifier))
			this.validateParallel = values[0].booleanValue();
		else if (HyFD.Identifier.ENABLE_MEMORY_GUARDIAN.name().equals(identifier))
			this.memoryGuardian.setActive(values[0].booleanValue());
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
		if (HyFD.Identifier.MAX_DETERMINANT_SIZE.name().equals(identifier))
			this.maxLhsSize = values[0].intValue();
		else if (HyFD.Identifier.INPUT_ROW_LIMIT.name().equals(identifier))
			if (values.length > 0)
				this.inputRowLimit = values[0].intValue();
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
		if (HyFD.Identifier.INPUT_GENERATOR.name().equals(identifier))
			this.inputGenerator = values[0];
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}*/
	

	/*@Override
	public String toString() {
		return "HyFD:\r\n\t" + 
				"inputGenerator: " + ((this.inputGenerator != null) ? this.inputGenerator.toString() : "-") + "\r\n\t" +
				"tableName: " + this.tableName + " (" + CollectionUtils.concat(this.attributeNames, ", ") + ")\r\n\t" +
				"numAttributes: " + this.numAttributes + "\r\n\t" +
				"isNullEqualNull: " + ((this.valueComparator != null) ? String.valueOf(this.valueComparator.isNullEqualNull()) : "-") + ")\r\n\t" +
				"maxLhsSize: " + this.maxLhsSize + "\r\n" +
				"inputRowLimit: " + this.inputRowLimit + "\r\n" +
				"\r\n" +
				"Progress log: \r\n" + Logger.getInstance().read();
	}*/
	
	public static void execute(){
		long startTime = System.currentTimeMillis();
		valueComparator = new ValueComparator(true);
		//this.executeFDEP();
		executeHyFD();
		
		Logger.getInstance().writeln("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	public static void executeHyFD(){
		// Initialize
		Logger.getInstance().writeln("Initializing ...");
		loadData();
		maxLhsSize = numberAttributes - 1;
		///////////////////////////////////////////////////////
		// Build data structures for sampling and validation //
		///////////////////////////////////////////////////////
		
		// Calculate plis
		Logger.getInstance().writeln("Reading data and calculating plis ...");
		PLIBuilder pliBuilder = new PLIBuilder(numberTuples);
		List<PositionListIndex> plis = pliBuilder.getPLIs(tuples, numberAttributes, valueComparator.isNullEqualNull());

		final int numRecords = pliBuilder.getNumLastRecords();
		System.out.println("numRecords: "+numRecords);
		
		pliBuilder = null;
		
		// TODO: seems like handling an edge case come to this later
		/*if (numRecords == 0) {
			ObjectArrayList<ColumnIdentifier> columnIdentifiers = this.buildColumnIdentifiers();
			for (int attr = 0; attr < this.numAttributes; attr++)
				this.resultReceiver.receiveResult(new FunctionalDependency(new ColumnCombination(), columnIdentifiers.get(attr)));
			return;
		}*/
		
		// Sort plis by number of clusters: For searching in the covers and for validation, it is good to have attributes with few non-unique values and many clusters left in the prefix tree
		Logger.getInstance().writeln("Sorting plis by number of clusters ...");
		Collections.sort(plis, new Comparator<PositionListIndex>() { // pli with more clusters, or more unique values apear first
			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {		
				int numClustersInO1 = numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 = numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});
		
		// Calculate inverted plis
		Logger.getInstance().writeln("Inverting plis ...");
		int[][] invertedPlis = invertPlis(plis, numRecords);
		// DEBUG
		/*for(int i = 0; i < plis.size(); i++)
		{
			System.out.println("pli "+i+" | "+plis.get(i).toString());
		}*/
				
		// DEBUG
		/*System.out.println("Inverted PLIs");
		for(int i = 0; i < invertedPlis.length; i++)
		{
			System.out.print("i= "+i+" | ");
			for(int j = 0; j < invertedPlis[i].length; j++)
			{
				System.out.print(invertedPlis[i][j]+" ");
			}
			System.out.println();
		}*/
		
		// Extract the integer representations of all records from the inverted plis
		Logger.getInstance().writeln("Extracting integer representations for the records ...");
		int[][] compressedRecords = new int[numRecords][];
		for (int recordId = 0; recordId < numRecords; recordId++)
			compressedRecords[recordId] = fetchRecordFrom(recordId, invertedPlis);
		invertedPlis = null;
		
		// DEBUG
		/*System.out.println("Compressed Records");
		for(int i = 0; i < compressedRecords.length; i++)
		{
			System.out.print("i= "+i+" | ");
			for(int j = 0; j < compressedRecords[i].length; j++)
			{
				System.out.print(compressedRecords[i][j]+" ");
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
		
		Sampler sampler = new Sampler(negCover, posCover, compressedRecords, plis, efficiencyThreshold, valueComparator, memoryGuardian);
		Inductor inductor = new Inductor(negCover, posCover, memoryGuardian);
		Validator validator = new Validator(negCover, posCover, numRecords, compressedRecords, plis, efficiencyThreshold, validateParallel, memoryGuardian);
		
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		do {
			/*for(IntegerPair ip : comparisonSuggestions)
				System.out.println(" - - comparisonSuggestions: "+ ip.a() + " "+ip.b());*/
			FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
			System.out.println(" #### of non FDs: " + newNonFds.size());
			
			// DEBUG
			/*System.out.println(" Print NegCover: ");
			for(ObjectOpenHashSet<OpenBitSet> hs : negCover.getFdLevels()) {
				for(OpenBitSet b : hs) {
					for(int i = 0; i < posCover.getNumAttributes(); i++)
						if(b.get(i))
							System.out.print(i+" ");
					System.out.println();
				}
			}*/
						
			inductor.updatePositiveCover(newNonFds);
			System.out.println(" #### of FDs: " + posCover.writeFunctionalDependencies(new OpenBitSet(), buildColumnIdentifiers(), false));
			comparisonSuggestions = validator.validatePositiveCover();
		}
		while (comparisonSuggestions != null);
		negCover = null;
		
		// Output all valid FDs
		Logger.getInstance().writeln("Translating FD-tree into result format ...");
		
		int numFDs = posCover.writeFunctionalDependencies(new OpenBitSet(), buildColumnIdentifiers(), false);
	//	int numFDs = posCover.addFunctionalDependenciesInto(resultReceiver, this.buildColumnIdentifiers(), plis);
		
		Logger.getInstance().writeln("... done! (" + numFDs + " FDs)");
	}

	private static ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(columnNames.length);
		for (String attributeName : columnNames)
			columnIdentifiers.add(new ColumnIdentifier(datasetFile, attributeName));
		return columnIdentifiers;
	}
	
	private static void loadData() {
        tuples = new ArrayList<List<String>>();
        
        List<Row> table = df.collectAsList();
        //df.cache();
        numberTuples = df.count();

        int tupleId = 0;
        //Read and paste data into TranslationMaps
        while (tupleId < numberTuples) {
           Row row = table.get(tupleId);
           List<String> row_as_list = new ArrayList<String>();
           for (int i = 0; i < numberAttributes; i++) {
              //System.out.println(((Long)row.get(i)).intValue());

              String content = row.get(i).toString();
              row_as_list.add(content);
           }
           tuples.add(row_as_list);
           tupleId++;
           if(tupleId%100000 == 0) System.out.println("Copied tuple: " + tupleId);
        }
    }

	private static int[][] invertPlis(List<PositionListIndex> plis, int numRecords) {
		int[][] invertedPlis = new int[plis.size()][];
		for (int attr = 0; attr < plis.size(); attr++) {
			int[] invertedPli = new int[numRecords];
			Arrays.fill(invertedPli, -1);
			
			for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
				for (int recordId : plis.get(attr).getClusters().get(clusterId))
					invertedPli[recordId] = clusterId;
			}
			invertedPlis[attr] = invertedPli;
		}
		return invertedPlis;
	}
	
	private static int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
		int[] record = new int[numberAttributes];
		for (int i = 0; i < numberAttributes; i++)
			record[i] = invertedPlis[i][recordId];
		return record;
	}
}
